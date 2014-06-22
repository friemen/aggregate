(ns aggregate.core
  (:refer-clojure :exclude [load])
  (:require [clojure.java.jdbc :as jdbc]
            [parsargs.core :as p]))

;; TODO
;; - Introduce better logging
;; - Create a find-by function


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
;; * An er-config is a map with :entities and :options, :entities is a
;;   map {entity-kw -> entity-spec}.
;;
;; * An entity-spec is a map that contains :options and :relations, where
;;   :relations is a map {relation-kw -> relation-spec}.
;;   The options map contains functions for reading, inserting, updating or
;;   deleting records.
;;
;; * A relation-spec is a map with the keys
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
  [id-kw insert-result]
  (let [record (first insert-result)]
    (or (get record id-kw) (-> record vals first))))


(defn persisted?
  "Returns true if the map m has already been persisted.
  It is assumed that the existence of an id-kw key is enough evidence."
  [id-kw m]
  (contains? m id-kw))


(defn make-read-fn
  "Returns a read function [db-spec id -> row-map] for a specific table.
  It returns a single record or nil. The tablename may be passed as
  string or keyword."
  [tablename id-kw]
  {:pre [id-kw]}
  (fn [db-spec id]
    (first
     (jdbc/query db-spec
                 [(str "select * from " (name tablename) " where " (name id-kw) " = ?") id]))))


(defn make-insert-fn
  "Returns an insert function [db-spec row-map] for a specific table.
  It returns the record, possibly augmented with the generated id in
  an :id slot. The tablename may be passed as string or keyword."
  [tablename id-kw]
  {:pre [id-kw]}
  (fn [db-spec row-map]
    (let [id (->> (jdbc/insert! db-spec
                                (keyword tablename)
                                row-map)
                  (extract-id id-kw))]
      (assoc row-map id-kw id))))


(defn make-update-fn
  "Returns an update function [db-spec set-map -> set-map] for a
  specific table, which takes a map of values and updates the record
  denoted by the :id contained in set-map. It returns the set-map.
  The tablename may be passed as string or keyword."
  [tablename id-kw]
  {:pre [id-kw]}
  (fn [db-spec set-map]
    {:pre [(get set-map id-kw)]}
    (jdbc/update! db-spec
                  (keyword tablename)
                  set-map
                  [(str (name id-kw) " = ?") (get set-map id-kw)])
    set-map))


(defn make-delete-fn
  "Returns a delete function [db-spec id -> (Seq id)] for a specific
  table, which deletes the record id points to. It returns the number
  of deleted records (usually 1), or nil if none was deleted.
  The tablename may be passed as string or keyword."
  [tablename id-kw]
  {:pre [id-kw]}
  (fn [db-spec id]
    (let [n (first (jdbc/delete! db-spec
                                 (keyword tablename)
                                 [(str (name id-kw) " = ?") id]))]
      (if (> n 0) n nil))))



(defn make-query-<many-fn
  "Returns a finder function [db-spec id -> (Seq Map)] for a specific
  table, that returns -- as sequence of maps -- all records that have
  id as value of the foreign key field denoted by fk-kw. 
  Assumes a simple n to 1 relationship."
  [tablename fk-kw]
  {:pre [fk-kw]}
  (fn [db-spec id]
    (jdbc/query db-spec [(str "select * from " (name tablename)
                              " where " (name fk-kw) " = ?") id])))


(defn make-query-<many>-fn
  "Returns a finder function [db-spec id -> (Seq Map)] for a
  specific table whose records are in a m to n relationship
  realized by a link table containing two foreign keys. The
  function returns a sequence of maps."
  ([tablename linktablename fk-a fk-b]
     (make-query-<many>-fn tablename linktablename fk-a fk-b :id))
  ([tablename linktablename fk-a fk-b b-id-kw]
     {:pre [fk-a fk-b]}
     (let [sql (str "select * from " (name tablename)
                    " A join " (name linktablename) " AB"
                    " on A." (name b-id-kw) " = AB." (name fk-b)
                    " where AB." (name fk-a) " = ?")]
       (fn [db-spec id-b]
         (->> (jdbc/query db-spec [sql id-b])
              (map #(dissoc % (keyword fk-a) (keyword fk-b))))))))


(defn make-update-links-fn
  "Returns a function that updates a link table by first deleting all
  records having a-id as value in the field fk-a, and afterwards
  inserting for each of the bs one record with fk-a = a-id and fk-b
  = (:id b)."
  ([linktablename fk-a fk-b]
     (make-update-links-fn linktablename fk-a fk-b :id))
  ([linktablename fk-a fk-b b-id-kw]
     {:pre [fk-a fk-b b-id-kw]}
     (fn [db-spec a-id bs]
       (jdbc/delete! db-spec (keyword linktablename) [(str (name fk-a) " = ?") a-id])
       (doseq [b bs]
         (jdbc/insert! db-spec (keyword linktablename) {(keyword fk-a) a-id
                                                        (keyword fk-b) (get b b-id-kw)})))))



(defn make-entity-options
  "Returns a map containing all four default JDBC based implementations
  for read, insert, update and delete."
  ([tablename]
     (make-entity-options tablename :id))
  ([tablename id-kw]
     {:pre [id-kw]}
     {:id-kw id-kw
      :read-fn (make-read-fn tablename id-kw)
      :insert-fn (make-insert-fn tablename id-kw)
      :update-fn (make-update-fn tablename id-kw)
      :delete-fn (make-delete-fn tablename id-kw)}))


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


(defn- with-default-entity-options
  "Returns a map containing all four default JDBC based implementations
  for read, insert, update and delete."
  [er-config
   entity-kw
   {:keys [read-fn insert-fn update-fn delete-fn]}]
  (let [{:keys [id-kw
                read-fn-factory
                insert-fn-factory
                update-fn-factory
                delete-fn-factory]} (-> er-config :options)
                e-id-kw (or (-> er-config :entities entity-kw :options :id-kw) id-kw :id)]
    {:id-kw e-id-kw
     :read-fn (or read-fn (read-fn-factory entity-kw e-id-kw))
     :insert-fn (or insert-fn (insert-fn-factory entity-kw e-id-kw))
     :update-fn (or update-fn (update-fn-factory entity-kw e-id-kw))
     :delete-fn (or delete-fn (delete-fn-factory entity-kw e-id-kw))}))


(defn- with-default-relation-fns
  "Returns a pair [relation-kw relation] with default functions added where missing."
  [er-config
   parent-entity-kw
   [relation-kw {:keys [relation-type entity-kw fk-kw query-fn update-links-fn] :as relation}]]
  (let [{:keys [query-<many-fn-factory
                query-<many>-fn-factory
                update-links-fn-factory]} (-> er-config :options)]
    (vector relation-kw
            (case relation-type
              :one> relation
              :<many (let [fk-kw (or fk-kw (default-fk parent-entity-kw))]
                       (assoc relation
                         :fk-kw fk-kw
                         :query-fn (or query-fn
                                       (query-<many-fn-factory entity-kw fk-kw))))
              :<many> (let [fk-a (default-fk parent-entity-kw)
                            fk-b (default-fk entity-kw)
                            b-id-kw (-> er-config :entities entity-kw :options :id-kw)]
                        (assoc relation
                          :query-fn
                          (or query-fn
                              (query-<many>-fn-factory
                               entity-kw
                               (default-link-tablename parent-entity-kw entity-kw) fk-a fk-b b-id-kw))
                          :update-links-fn
                          (or update-links-fn
                              (update-links-fn-factory
                               (default-link-tablename parent-entity-kw entity-kw) fk-a fk-b b-id-kw))))))))


(defn- with-defaults
  "Returns an er-config with default functions added where missing."
  [er-config]
  (let [er-config'
        (update-in er-config [:entities]
                   (fn [entities]
                   (->> entities
                        (map (fn [[entity-kw {:keys [options relations]}]]
                                (vector entity-kw
                                        {:options (with-default-entity-options er-config entity-kw options)
                                         :relations relations})))
                        (into {}))))]    
    (update-in er-config' [:entities]
               (fn [entities]
                 (->> entities
                      (map (fn [[entity-kw {:keys [options relations]}]]
                             (vector entity-kw
                                     {:options options
                                      :relations (->> relations
                                                      (map (partial with-default-relation-fns er-config' entity-kw))
                                                      (into {}))})))
                      (into {}))))))


(defn- with-default-options
  "Returns the default options, replacing defaults by entries in the
  options-map."
  [er-options]
  (merge {:id-kw :id
          :persisted-pred-fn persisted?
          :read-fn-factory make-read-fn
          :insert-fn-factory make-insert-fn
          :update-fn-factory make-update-fn
          :delete-fn-factory make-delete-fn
          :query-<many-fn-factory make-query-<many-fn
          :query-<many>-fn-factory make-query-<many>-fn
          :update-links-fn-factory make-update-links-fn} er-options))


(defn- entityspec?
  "Returns true if x is a vector containing a keyword as first and a
  map as second item"
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
  arbitrary number of entity-specs, i.e. 
      (agg/make-er-config options-map? entities)
  Available options:
  :read-fn-factory          A function (fn [tablename]) returning the 
                            default read function.
  :insert-fn-factory        A function (fn [tablename]) returning the
                            default insert function.
  :update-fn-factory        A function (fn [tablename]) returning the
                            default update function.
  :delete-fn-factory        A function (fn [tablename]) returning the 
                            default delete function.
  :query-<many-fn-factory   A function (fn [tablename fk-kw]) returning
                            the default query-for-many function using
                            one foreign key.
  :query-<many>-fn-factory  A function (fn [tablename linktablename fk-a fk-b]) 
                            returning the default query-for-many function
                            that uses a linktable.
  :update-links-fn-factory  A function (fn [linktablename fk-a fk-b]) 
                            returning the default function to update 
                            link tables.
  :id-kw                    A keyword that is taken as default primary 
                            key column name.
  :persisted-pred-fn        A predicate that returns true if the given
                            row-map is already present in DB."
  [& args]
  (let [{:keys [options entity-specs]} (er-config-parser args)]
    (with-defaults {:options (with-default-options options)
                    :entities (into {} entity-specs)})))


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
  options-map and an arbitrary number of relation-specs, i.e.
      (agg/entity entity-kw options-map? relations)
  Available options:
  :read-fn       A function (fn [db-spec id]) returning the
                 record with primary key value id as a map.
  :insert-fn     A function (fn [db-spec row-map]) that inserts 
                 the row-map as record, and returns the row-map
                 containing the new primary key value.
  :update-fn     A function (fn [db-spec set-map]) that updates
                 the record identified by the primary key value 
                 within set-map with the values of set-map.
  :delete-fn     A function (fn [db-spec id]) that deletes the
                 record identified by the primary key value.
  :id-kw         The keyword to be used to get/assoc the primary 
                 key value. It is also used as primary key 
                 column name in default DB access functions."
  ([& args]
     (let [{:keys [entity-kw options relation-specs]} (entity-parser args)]
       (vector entity-kw {:options options
                          :relations (into {} relation-specs)}))))


(defn ->1
  "Returns a relation-spec for a :one> relationship.
  Available options:
  fk-kw             A keyword denoting the foreign-key name.
  :owned?           A boolean telling if the related entity is owned,
                    i.e. will be deleted when the owner or the link is
                    deleted. Defaults to true."
  ([relation-kw entity-kw]
     (->1 relation-kw entity-kw {}))
  ([relation-kw entity-kw options]
     (vector relation-kw (merge {:relation-type :one>
                                 :entity-kw entity-kw
                                 :fk-kw (default-fk relation-kw)
                                 :owned? true}
                                options))))


(defn ->n
  "Returns a relation-spec for a :<many relationship.
  Available options:
  fk-kw             A keyword denoting the foreign-key name.
  :query-fn         A function returning records by a foreign key.
  :owned?           A boolean telling if the related entity is owned,
                    i.e. will be deleted when the owner or the link is
                    deleted. Defaults to true."
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
  "Returns a relation-spec for a :<many> relationship.
  Available options:
  :query-fn         A function returning records by a foreign key.
  :update-links-fn  A function for updating link table records.
  :owned?           A boolean telling if the related entity is owned,
                    i.e. will be deleted when the owner or the link is
                    deleted. Defaults to false."
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
              (update-in er-config [:entities (first k-or-ks) :relations] dissoc-ks (rest k-or-ks))
              (update-in er-config [:entities] dissoc k-or-ks)))
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
            (update-in er-config [:entities (first ks) :relations] keep-ks (rest ks)))
          er-config
          entity-relation-seqs))

;;--------------------------------------------------------------------
;; Common utility functions for the big three: load, save! and delete!


(defn- without-relations-and-entity
  "Removes all key-value-pairs from m that correspond to relations."
  [er-config entity-kw m]
  (->> er-config :entities entity-kw :relations
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
  (contains? (:entities er-config) entity-kw))



(declare load save! delete!)


;;--------------------------------------------------------------------
;; Load aggregate


(defn- load-relation
  "Loads more data according to the specified relation."
  [er-config db-spec id-kw m
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
    (if (-> er-config :entities entity-kw)
      (assoc m
        relation-kw
        (->> (get m id-kw)
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
     (let [read-fn (-> er-config :entities entity-kw :options :read-fn)
           m (if read-fn (some-> (read-fn db-spec id)
                                 (assoc ::entity entity-kw)))]
       (load er-config db-spec m)))
  ([er-config db-spec m]
     (if-let [entity-kw (::entity m)]
       (let [id-kw (-> er-config :entities entity-kw :options :id-kw)
             relations (-> er-config :entities entity-kw :relations)]
         (reduce (partial load-relation (-> er-config (without entity-kw)) db-spec id-kw)
                 m
                 relations)))))



;;--------------------------------------------------------------------
;; Save aggregate


(defn- save-prerequisite!
  "Saves a record that m points to by a foreign-key."
  [er-config
   db-spec
   update-m-fn
   id-kw
   m
   [relation-kw {:keys [entity-kw fk-kw owned?]}]]
  (log "save-prerequisite" relation-kw "->" entity-kw)
  (if-let [p (relation-kw m)] ; does the prerequisite exist?
    ;; save the prerequisite record and take its id as foreign key 
    (let [id-kw (-> er-config :entities entity-kw :options :id-kw)
          persisted? (-> er-config :options :persisted-pred-fn)
          saved-p (save! er-config db-spec entity-kw p)]
      (assoc m
        fk-kw (get saved-p id-kw)
        relation-kw saved-p))
    ;; prerequisite does not exist
    (let [fk-id (fk-kw m)
          persisted? (-> er-config :options :persisted-pred-fn)]
      (when (and fk-id (persisted? id-kw m))
        ;; m is persisted and points to the prerequisite, so update m
        (update-m-fn db-spec {id-kw (get m id-kw) fk-kw nil}))
      (when owned?
        ;; delete the former prerequisite by the foreign key from DB
        (delete! er-config db-spec entity-kw fk-id))
      (if (persisted? id-kw m)
        (dissoc m fk-kw)
        m))))


(defn- save-dependants!
  "Saves records that point via foreign-key to m."
  [er-config
   db-spec
   id-kw
   m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn query-fn owned?]}]]
  (log "save-dependants" relation-kw "->" entity-kw)
  (let [m-id (get m id-kw)
        m-entity-kw (::entity m)
        dependants (let [d-id-kw (-> er-config :entities entity-kw :options :id-kw)
                         update-fn (-> er-config :entities entity-kw :options :update-fn)
                         current-ds (query-fn db-spec m-id)
                         saved-ds (->> (get m relation-kw)
                                       ;; insert foreign key value
                                       (map #(if (= relation-type :<many>)
                                               %
                                               (assoc % fk-kw m-id)))
                                       (map #(save! (-> er-config (without m-entity-kw))
                                                     db-spec
                                                     entity-kw
                                                     %))
                                       (mapv #(dissoc % fk-kw)))
                         saved-ds-ids (->> saved-ds (map #(get % d-id-kw)) set)]
                     ;; delete or unlink all orphans
                     (doseq [orphan (->> current-ds (remove #(saved-ds-ids (get % d-id-kw))))]
                       (if owned?
                         (delete! er-config db-spec entity-kw orphan)
                         (if (not= relation-type :<many>)
                           (update-fn db-spec (assoc orphan fk-kw nil)))))
                     (if (= relation-type :<many>)
                       ;; remove all links for m and insert new links
                       (update-links-fn db-spec m-id saved-ds))
                     saved-ds)]
    (if (-> er-config :entities entity-kw)
      ;; don't add empty collections for already processed entities
      (assoc m relation-kw dependants)
      m)))


(defn- ins-or-up!
  "Invokes either the :update or the :insert function, depending on whether
  m is persisted or not."
  [er-config db-spec entity-kw id-kw m]
  (let [persisted? (-> er-config :options :persisted-pred-fn)
        ins-or-up-fn (-> er-config :entities entity-kw :options
                         (get (if (persisted? id-kw m) :update-fn :insert-fn)))
        saved-m (->> m
                     (without-relations-and-entity er-config entity-kw)
                     (ins-or-up-fn db-spec))]
    (assoc m
      id-kw (get saved-m id-kw)
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
       (let [id-kw (-> er-config :entities entity-kw :options :id-kw)
             relations (-> er-config :entities entity-kw :relations)
             update-fn (-> er-config :entities entity-kw :options :update-fn)
             m (->> relations
                    (filter (partial rt? :one>))
                    (filter (partial rr? er-config))
                    (reduce (partial save-prerequisite! (-> er-config (without entity-kw)) db-spec update-fn id-kw) m)
                    ;; this will persist m itself (containing all foreign keys)
                    (ins-or-up! er-config db-spec entity-kw id-kw))]
         ;; process all other types of relations
         ;; that require m's id as value for the foreign key
         (->> relations
              (remove (partial rt? :one>))
              (filter (partial rr? er-config))
              (reduce (partial save-dependants! (-> er-config (without entity-kw)) db-spec id-kw) m))))))
                                       

;;--------------------------------------------------------------------
;; Delete aggregate

(defn- nil->0
  [n]
  (if (number? n) n 0))


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
  [er-config db-spec id-kw m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn owned?]}]]
  (log "delete dependants" relation-kw "->" entity-kw)
  (let [deleted (if owned?
                  (->> (get m relation-kw)
                       (map (partial delete! er-config db-spec entity-kw))
                       (map nil->0)
                       (apply +))
                  (if (= relation-type :<many)
                    (let [d-id-kw (-> er-config :entities entity-kw :options :id-kw)
                          update-fn (-> er-config :entities entity-kw :options :update-fn)]
                      (->> (get m relation-kw)
                           (map #(update-fn db-spec {d-id-kw (get % d-id-kw) fk-kw nil}))
                           doall)
                      0)
                    0))]
    (when (= relation-type :<many>)
      (update-links-fn db-spec (get m id-kw) []))
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
       (let [id-kw (-> er-config :entities entity-kw :options :id-kw)
             delete-fn (-> er-config :entities entity-kw :options :delete-fn)]
         (if (map? m-or-id)
           (let [;; delete all records that might point to m
                 deleted-dependants (->> er-config :entities entity-kw :relations
                                         (remove (partial rt? :one>))
                                         (map (partial delete-dependants! er-config db-spec id-kw m-or-id))
                                         (map nil->0)
                                         (apply +))
                 ;; delete the record
                 deleted-m (nil->0 (delete-fn db-spec (get m-or-id id-kw)))
                 ;; delete all owned records that m points to via foreign key
                 deleted-prerequisites (->> er-config :entities entity-kw :relations
                                            (filter (partial rt? :one>))
                                            (map (partial delete-prerequisite! er-config db-spec m-or-id))
                                            (map nil->0)
                                            (apply +))]
             (+ deleted-dependants deleted-m deleted-prerequisites))
           (delete-fn db-spec m-or-id)))
       0)))
