(ns aggregate.core
  (:refer-clojure :exclude [load])
  (:require [clojure.java.jdbc :as jdbc]
            [parsargs.core :as p]))

;; TODO
;; - Introduce logging
;; - Make detection if insert or update is necessary configurable
;; - Make :id keyword configurable on a per entity basis
;; - Create a load-all function


;; -------------------------------------------------------------------
;; For testing in the REPL


;; This will start an in-memory DB instance
#_ (def con {:connection
             (jdbc/get-connection
              {:classname "org.h2.Driver"
               :subprotocol "h2"
               :subname "mem:repl"
               :user "sa"
               :password ""})})

;; Alternatively, this will start a DB server
#_ (do (require '[aggregate.h2 :as h2])
       (require '[aggregate.core :as agg])
       (h2/start-db))

;; Subsequently you can use @h2/db-con, which will use the DB server
;; To inspect the state of the DB use http://localhost:8082



;;--------------------------------------------------------------------
;; Concepts
;;
;; * An er-config is a map of entities.
;;
;; * An entity is a keyword pointing to a map that contains functions for
;;   reading, inserting, updating or deleting records, and contains
;;   relations descriptions to other entities.
;;
;; * A relation is a map with the keys
;;   - relation-type    Admissible values are :one>, :<many and :<many>
;;   - entity-kw        A keyword denoting an entity
;;   - fk-kw            A keyword denoting the foreign-key name
;;   - owned?           A boolean telling if the related entity is owned,
;;                      i.e. will be deleted when the owner or the link is
;;                      deleted.
;;   - query-fn         A function returning records by a foreign key.
;;                      (only relevant for :<many and :<many>)
;;   - update-links-fn  A function for updating link table records.
;;                      (only relevant for :<many>)

;;--------------------------------------------------------------------
;; Common utilities


(defn- log
  [& xs]
  #_ (apply println xs)
  xs)

;;--------------------------------------------------------------------
;; Factories for default DB access functions based on clojure.java.jdbc


(defn extract-id
  "Extracts id value from results like ({:scope_identity() 2}) or ({:id 2, ...})."
  [insert-result]
  (let [record (first insert-result)]
    (or (:id record) (-> record vals first))))


(defn make-read-fn
  "Returns a read function [db-spec id -> row-map] for a specific table.
  It returns a single record or nil. The tablename may be passed as
  string or keyword."
  [tablename]
  (fn [db-spec id]
    (first
     (jdbc/query db-spec
                 [(str "select * from " (name tablename) " where id = ?") id]))))


(defn make-insert-fn
  "Returns an insert function [db-spec row-map] for a specific table.
  It returns the record, possibly augmented with the generated id in
  an :id slot. The tablename may be passed as string or keyword."
  [tablename]
  (fn [db-spec row-map]
    (let [id (->> (jdbc/insert! db-spec
                                (keyword tablename)
                                row-map)
                  extract-id)]
      (assoc row-map :id id))))


(defn make-update-fn
  "Returns an update function [db-spec set-map -> set-map] for a
  specific table, which takes a map of values and updates the record
  denoted by the :id contained in set-map. It returns the set-map.
  The tablename may be passed as string or keyword."
  [tablename]
  (fn [db-spec set-map]
    {:pre [(:id set-map)]}
    (jdbc/update! db-spec
                  (keyword tablename)
                  set-map
                  ["id = ?" (:id set-map)])
    set-map))


(defn make-delete-fn
  "Returns a delete function [db-spec id -> (Seq id)] for a specific
  table, which deletes the record id points to. It returns the number
  of deleted records (usually 1), or nil if none was deleted.
  The tablename may be passed as string or keyword."
  [tablename]
  (fn [db-spec id]
    (let [n (first (jdbc/delete! db-spec
                                 (keyword tablename)
                                 ["id = ?" id]))]
      (if (> n 0) n nil))))



(defn make-query-<many-fn
  "Returns a finder function [db-spec id -> (Seq Map)] for a specific
  table, that returns -- as sequence of maps -- all records that have
  id as value of the foreign key field denoted by fk-kw. 
  Assumes a simple n to 1 relationship."
  [tablename fk-kw]
  (fn [db-spec id]
    (jdbc/query db-spec [(str "select * from " (name tablename)
                              " where " (name fk-kw) " = ?") id])))


(defn make-query-<many>-fn
  "Returns a finder function [db-spec id -> (Seq Map)] for a
  specific table whose records are in a m to n relationship
  realized by a link table containing two foreign keys. The
  function returns a sequence of maps."
  [tablename linktablename fk-a fk-b]
  (let [sql (str "select * from " (name tablename)
                 " A join " (name linktablename) " AB"
                 " on A.id = AB." (name fk-b)
                 " where AB." (name fk-a) " = ?")]
    (fn [db-spec id-b]
      (->> (jdbc/query db-spec [sql id-b])
           (map #(dissoc % (keyword fk-a) (keyword fk-b)))))))


(defn make-update-links-fn
  "Returns a function that updates a link table by first deleting all
  records having a-id as value in the field fk-a, and afterwards
  inserting for each of the bs one record with fk-a = a-id and fk-b
  = (:id b)."
  [linktablename fk-a fk-b]
  (fn [db-spec a-id bs]
    (jdbc/delete! db-spec (keyword linktablename) [(str (name fk-a) " = ?") a-id])
    (doseq [b bs]
      (jdbc/insert! db-spec (keyword linktablename) {(keyword fk-a) a-id
                                                     (keyword fk-b) (:id b)}))))

(defn make-entity-fns
  "Returns a map containing all four default JDBC based implementations
  for read, insert, update and delete."
  [tablename]
  {:read (make-read-fn tablename)
   :insert (make-insert-fn tablename)
   :update (make-update-fn tablename)
   :delete (make-delete-fn tablename)})


;;--------------------------------------------------------------------
;; Helpers for succinctly creating er-config map


(defn- default-fk
  "Returns a keyword that has the suffix '_id'."
  [entity-kw]
  (-> entity-kw name (str "_id") keyword))


(defn- default-link-tablename
  "Takes two tablenames and joins them with '_' and returns the result as keyword."
  [a-entity-kw b-entity-kw]
  (keyword (str (name a-entity-kw) "_" (name b-entity-kw))))


(defn with-default-entity-fns
  "Returns a map containing all four default JDBC based implementations
  for read, insert, update and delete."
  [{:keys [read-fn-factory insert-fn-factory update-fn-factory delete-fn-factory]}
   tablename
   {:keys [read insert update delete]}]
  {:read (or read (read-fn-factory tablename))
   :insert (or insert (insert-fn-factory tablename))
   :update (or update (update-fn-factory tablename))
   :delete (or delete (delete-fn-factory tablename))})


(defn- with-default-relation-fns
  "Returns a pair [relation-kw relation] with default functions added where missing."
  [{:keys [query-<many-fn-factory
           query-<many>-fn-factory
           update-links-fn-factory]}
   parent-entity-kw
   [relation-kw {:keys [relation-type entity-kw fk-kw query-fn update-links-fn] :as relation}]]
  (vector relation-kw
          (case relation-type
            :one> relation
            :<many (let [fk-kw (or fk-kw (default-fk parent-entity-kw))]
                     (assoc relation
                       :fk-kw fk-kw
                       :query-fn (or query-fn
                                     (query-<many-fn-factory entity-kw fk-kw))))
            :<many> (let [fk-a (default-fk parent-entity-kw)
                          fk-b (default-fk entity-kw)]
                      (assoc relation
                        :query-fn
                        (or query-fn
                            (query-<many>-fn-factory
                             entity-kw
                             (default-link-tablename parent-entity-kw entity-kw) fk-a fk-b))
                        :update-links-fn
                        (or update-links-fn
                            (update-links-fn-factory
                             (default-link-tablename parent-entity-kw entity-kw) fk-a fk-b)))))))


(defn- with-default-fns
  "Returns an er-config with default functions added where missing."
  [er-config]
  (let [options-map (::options er-config)]
    (assoc (->> er-config
                (remove #(= ::options (first %)))
                (map (fn [[entity-kw {:keys [fns relations] :as entity-spec}]]
                       (vector entity-kw
                               {:fns (with-default-entity-fns options-map entity-kw fns)
                                :relations (->> relations
                                                (map (partial with-default-relation-fns options-map entity-kw))
                                                (into {}))})))
                (into {}))
      ::options options-map)))


(defn- with-default-options
  "Returns the default options, replacing defaults by entries in the
  options-map."
  [options-map]
  (merge {:read-fn-factory make-read-fn
          :insert-fn-factory make-insert-fn
          :update-fn-factory make-update-fn
          :delete-fn-factory make-delete-fn
          :query-<many-fn-factory make-query-<many-fn
          :query-<many>-fn-factory make-query-<many>-fn
          :update-links-fn-factory make-update-links-fn} options-map))


(defn- entityspec?
  "Returns true if x is a vector containing a keyword as first and a map as second item"
  [x]
  (and (vector? x)
       (keyword? (first x))
       (map? (second x))))


(def ^:private er-config-parser
  (partial p/parse
           (p/sequence :options (p/optval map? {})
                       :entity-specs (p/some (p/value entityspec?)))))

(defn make-er-config
  "Creates a er-config map from an optional options-map and an
  arbitrary number of entity-specs."
  [& args]
  (let [{:keys [options entity-specs]} (er-config-parser args)]
    (with-default-fns (merge  {::options (with-default-options options)}
                              (into {} entity-specs)))))


(defn- relationspec?
  "Returns true if x is a vector, where the first item is a keyword
  and the second is a map containing :relation-type."
  [x]
  (and (vector? x)
       (keyword? (first x))
       (contains? (second x) :relation-type)))


(def ^:private entity-parser
  (partial p/parse
           (p/sequence :entity-kw (p/value keyword?)
                       :options (p/optval #(and (map? %) (nil? (:relation-type %))) {})
                       :relation-specs (p/some (p/value relationspec?)))))

(defn entity
  "Returns an entity-spec from an entity keyword, an optional
  options-map and an arbitrary number of relation-specs. Admissible
  options are:
  :fns  A map with functions for :read, :insert, :update and :delete."
  ([& args]
     (let [{:keys [entity-kw options relation-specs]} (entity-parser args)]
       (vector entity-kw (merge {:fns (-> options :fns)}
                                {:relations (into {} relation-specs)})))))


(defn ->1
  "Returns a relation-spec for a :one> relationship."
  ([relation-kw entity-kw]
     (->1 relation-kw entity-kw {}))
  ([relation-kw entity-kw options]
     (vector relation-kw (merge {:relation-type :one>
                                 :entity-kw entity-kw
                                 :fk-kw (default-fk relation-kw)
                                 :owned? true}
                                options))))


(defn ->n
  "Returns a relation-spec for a :<many relationship."
  ([relation-kw entity-kw]
     (->n relation-kw entity-kw {}))
  ([relation-kw entity-kw options]
     (vector relation-kw (merge {:relation-type :<many
                                 :entity-kw entity-kw
                                 :fk-kw nil
                                 :query-fn nil
                                 :owned? true}
                                options))))


(defn ->mn
  "Returns a relation-spec for a :<many> relationship."
  ([relation-kw entity-kw]
     (->mn relation-kw entity-kw {}))
  ([relation-kw entity-kw options]
     (vector relation-kw (merge {:relation-type :<many>
                                 :entity-kw entity-kw
                                 :query-fn nil
                                 :update-links-fn nil
                                 :owned? false}
                                options))))

(defn- dissoc-ks
  [m ks]
  (apply (partial dissoc m) ks))


(defn without
  "Removes entities (specified by a keyword) and relations (specified
  in a vector, where the first item is the entity keyword) from the er-config."
  [er-config & entities-or-entity-relation-seqs]
  (reduce (fn [er-config k-or-ks]
            (if (coll? k-or-ks)
              (update-in er-config [(first k-or-ks) :relations] dissoc-ks (rest k-or-ks))
              (dissoc er-config k-or-ks)))
          er-config
          entities-or-entity-relation-seqs))


(defn- keep-ks
  [m ks]
  (let [ks-set (set ks)]
    (into {} (filter (comp ks-set first) m))))


(defn only
  "Removes all relations that are NOT specified by the vectors.
  A vector must begin with an entity-kw, all remaining items denote
  relations."
  [er-config & entity-relation-seqs]
  (reduce (fn [er-config ks]
            (update-in er-config [(first ks) :relations] keep-ks (rest ks)))
          er-config
          entity-relation-seqs))

;;--------------------------------------------------------------------
;; Common utility functions for the big three load, save! and delete!


(defn- without-relations-and-entity
  "Removes all key-value-pairs from m that correspond to relations."
  [er-config entity-kw m]
  (->> er-config
       entity-kw
       :relations
       keys
       (cons ::entity)
       (apply (partial dissoc m))))


(defn- rt?
  "Is relation of type? 
  Returns true if the relation-type equals t."
  [t [_ {:keys [relation-type]}]]
  (= t relation-type))

(defn- rr?
  "Is relation relevant? 
  Returns true if the relation points to an existing entity description."
  [er-config [_ {:keys [entity-kw]}]]
  (contains? er-config entity-kw))


(defn- persisted?
  "Returns true if the map has already been persisted.
  It is assumed that the existence of an :id key is enough evidence."
  [m]
  (contains? m :id))



(declare load save! delete!)


;;--------------------------------------------------------------------
;; Load aggregate


(defn- load-relation
  "Loads more data according to the specified relation."
  [er-config db-spec m
   [relation-kw
    {:keys [relation-type entity-kw fk-kw query-fn]}]]
  (case relation-type
    :one>
    (let [child-id (get m fk-kw)
          child (load er-config db-spec entity-kw child-id)]
      (if child
        (assoc m relation-kw child)
        (dissoc m relation-kw fk-kw)))
    (:<many :<many>)
    (if (er-config entity-kw)
      (assoc m
        relation-kw
        (->> m :id
             (query-fn db-spec)
             (map #(assoc % ::entity entity-kw))
             ;; invoke load for each record returned by the query
             (mapv (partial load er-config db-spec))))
      m)))


(defn load
  "Loads an aggregate by id, the entity-kw denotes the aggregate root.
  Returns a map containing the entity-kw in ::entity and all data, or 
  nil if the entity-kw is unknown or the record does not exist."
  ([er-config db-spec entity-kw id]
     (let [read-fn (-> er-config entity-kw :fns :read)
           m (if read-fn (some-> (read-fn db-spec id)
                                 (assoc ::entity entity-kw)))]
       (load er-config db-spec m)))
  ([er-config db-spec m]
     (if-let [entity-kw (::entity m)]
       (->> er-config
            entity-kw
            :relations
            (reduce (partial load-relation (dissoc er-config entity-kw) db-spec)
                    m)))))



;;--------------------------------------------------------------------
;; Save aggregate


(defn- save-prerequisite!
  "Saves a record that m points to by a foreign-key."
  [er-config
   db-spec
   update-m-fn
   m
   [relation-kw {:keys [entity-kw fk-kw owned?]}]]
  (log "save-prerequisite" relation-kw "->" entity-kw)
  (if-let [p (relation-kw m)] ; does the prerequisite exist?
    ;; save the prerequisite record and take its id as foreign key 
    (let [saved-p (save! er-config db-spec entity-kw p)]
      (assoc m
        fk-kw (:id saved-p)
        relation-kw saved-p))
    ;; prerequisite does not exist
    (let [fk-id (fk-kw m)]
      (when (and fk-id (persisted? m))
        ;; m is persisted and points to the prerequisite, so update m
        (update-m-fn db-spec {:id (:id m) fk-kw nil}))
      (when owned?
        ;; delete the former prerequisite by the foreign key from DB
        (delete! er-config db-spec entity-kw fk-id))
      (if (persisted? m)
        (dissoc m fk-kw)
        m))))


(defn- save-dependants!
  "Saves records that point via foreign-key to m."
  [er-config
   db-spec
   m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn query-fn owned?]}]]
  {:pre [(persisted? m)]}
  (log "save-dependants" relation-kw "->" entity-kw)
  (let [m-id (:id m)
        m-entity-kw (::entity m)
        dependants (let [update-fn (-> er-config entity-kw :fns :update)
                         current-ds (query-fn db-spec m-id)
                         saved-ds (->> (get m relation-kw)
                                       ;; insert foreign key value
                                       (map #(if (= relation-type :<many>)
                                               %
                                               (assoc % fk-kw m-id)))
                                       (map #(save! (dissoc er-config m-entity-kw)
                                                     db-spec
                                                     entity-kw
                                                     %))
                                       (mapv #(dissoc % fk-kw)))
                         saved-ds-ids (->> saved-ds (map :id) set)]
                     ;; delete or unlink all orphans
                     (doseq [orphan (->> current-ds (remove #(saved-ds-ids (:id %))))]
                       (if owned?
                         (delete! er-config db-spec entity-kw orphan)
                         (if (not= relation-type :<many>)
                           (update-fn db-spec (assoc orphan fk-kw nil)))))
                     (if (= relation-type :<many>)
                       ;; remove all links for m and insert new links
                       (update-links-fn db-spec m-id saved-ds))
                     saved-ds)]
    (if (er-config entity-kw)
      ;; don't add empty collections for already processed entities
      (assoc m relation-kw dependants)
      m)))


(defn- ins-or-up!
  "Invokes either the :insert or the :update function, depending on whether
  the :id value is nil or non-nil, respectively."
  [er-config db-spec entity-kw m]
  (let [ins-or-up-fn (-> er-config
                         entity-kw
                         :fns
                         (get (if (persisted? m) :update :insert)))
        saved-m (->> m
                     (without-relations-and-entity er-config entity-kw)
                     (ins-or-up-fn db-spec))]
    (assoc m
      :id (:id saved-m)
      ::entity entity-kw)))


(defn save!
  "Saves an aggregate data structure to the database."
  ([er-config db-spec m]
     {:pre [(::entity m)]}
     (save! er-config db-spec (::entity m) m))
  ([er-config db-spec entity-kw m]
     {:pre [(or (nil? m) (map? m))]}
     (log "save" entity-kw)
     (when m
       ;; first process all records linked with a :one> relation-type
       ;; because we need their ids as foreign keys in m
       (let [relations (-> er-config entity-kw :relations)
             update-fn (-> er-config entity-kw :fns :update)
             m (->> relations
                    (filter (partial rt? :one>))
                    (filter (partial rr? er-config))
                    (reduce (partial save-prerequisite! (dissoc er-config entity-kw) db-spec update-fn) m)
                    ;; this will persist m itself (containing all foreign keys)
                    (ins-or-up! er-config db-spec entity-kw))]
         ;; process all other types of relations
         ;; that require m's id as value for the foreign key
         (->> relations
              (remove (partial rt? :one>))
              (filter (partial rr? er-config))
              (reduce (partial save-dependants! (dissoc er-config entity-kw) db-spec) m))))))
                                       

;;--------------------------------------------------------------------
;; Delete aggregate

(defn- nil->0 [n] (if (number? n) n 0))

(defn- delete-prerequisite!
  "Deletes a record that m points to by a foreign key.
  Returns the number of deleted records or nil."
  [er-config db-spec m
   [relation-kw {:keys [relation-type entity-kw fk-kw owned?]}]]
  (log "delete prerequisite" relation-kw "->" entity-kw)
  (when owned?
    (let [fk-id (get m fk-kw)
          child (get m relation-kw)]
      (cond
       child (delete! er-config db-spec entity-kw child)
       fk-id (delete! er-config db-spec entity-kw fk-id)
       :else 0))))


(defn- delete-dependants!
  "Deletes all records that m contains, and that point by foreign key to m.
  Returns the number of deleted records."
  [er-config db-spec m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn owned?]}]]
  (log "delete dependants" relation-kw "->" entity-kw)
  (let [deleted (if owned?
                  (->> (get m relation-kw)
                       (map (partial delete! er-config db-spec entity-kw))
                       (map nil->0)
                       (apply +))
                  (if (= relation-type :<many)
                    (let [update-fn (-> er-config entity-kw :fns :update)]
                      (->> (get m relation-kw)
                           (map #(update-fn db-spec {:id (:id %) fk-kw nil}))
                           doall)
                      0)
                    0))]
    (when (= relation-type :<many>)
      (update-links-fn db-spec (:id m) []))
    deleted))


(defn delete!
  "Removes an aggregate datastructure from the database.
  Returns the number of deleted records."
  ([er-config db-spec m]
     {:pre [(::entity m)]}
     (delete! er-config db-spec (::entity m) m))
  ([er-config db-spec entity-kw m-or-id]
     (log "delete" entity-kw)
     (if m-or-id
       (let [delete-fn (-> er-config entity-kw :fns :delete)]
         (if (map? m-or-id)
           (let [;; delete all records that might point to m
                 deleted-dependants (->> er-config entity-kw :relations
                                         (remove (partial rt? :one>))
                                         (map (partial delete-dependants! er-config db-spec m-or-id))
                                         (map nil->0)
                                         (apply +))
                 ;; delete the record
                 deleted-m (nil->0 (delete-fn db-spec (:id m-or-id)))
                 ;; delete all owned records that m points to via foreign key
                 deleted-prerequisites (->> er-config entity-kw :relations
                                            (filter (partial rt? :one>))
                                            (map (partial delete-prerequisite! er-config db-spec m-or-id))
                                            (map nil->0)
                                            (apply +))]
             (+ deleted-dependants deleted-m deleted-prerequisites))
           (delete-fn db-spec m-or-id)))
       0)))
