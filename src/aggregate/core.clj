(ns aggregate.core
  (:refer-clojure :exclude [load])
  (:require [clojure.java.jdbc :as jdbc]))

;; TODO
;; - create helper fn for er-config creation
;; - find out how to map Game-Bet-Player-Gambler relations
;; - create a load-all function
;; - make :id keyword configurable per entity


;; -------------------------------------------------------------------
;; For testing in the REPL

;; This will start an im-memory DB instance
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
;; * An entity is a keyword pointing to a map that contains functions for
;;   reading, inserting, updating or deleting records, and contains
;;   relations descriptions to other entities.
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

(defn dump
  [x]
  (println x)
  x)


;;--------------------------------------------------------------------
;; Factories for default JDBC read, insert, update and delete functions

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
                  first vals first)]
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
                 " on A.id = AB." (name fk-a)
                 " where AB." (name fk-b) " = ?")]
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
;; Helper for succinctly creating er-config map

; TODO


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
  "Returns true if the relation-type equals t."
  [t [_ {:keys [relation-type]}]]
  (= t relation-type))


(declare load save! delete!)


;;--------------------------------------------------------------------
;; Load aggregate


(defn- load-relation
  [er-config db-spec m
   [relation-kw
    {:keys [relation-type entity-kw fk-kw query-fn]}]]
  (case relation-type
    :one>
    (let [child-id (get m fk-kw)
          child (load er-config
                      db-spec
                      entity-kw
                      child-id)]
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
            (reduce (partial load-relation
                             ;; remove current ent map
                             ;; to avoid infinite loops if relation definitions
                             ;; form a cycle
                             (dissoc er-config entity-kw)
                             db-spec)
                    m)))))



;;--------------------------------------------------------------------
;; Save aggregate



(defn- save-prerequisite!
  "Saves a record that m points to by a foreign-key."
  [er-config
   db-spec
   m
   [relation-kw {:keys [entity-kw fk-kw owned?]}]]
  (if-let [p (relation-kw m)] ; does the prerequisite exist?
    ;; save the prerequisite record and take it's id as foreign key 
    (let [saved-p (save! er-config db-spec entity-kw p)]
      (assoc m
        fk-kw (:id saved-p)
        relation-kw saved-p))
    (let [fk-id (fk-kw m)]
      (when owned?
        ;; delete the former prerequisite by the foreign key from DB
        (delete! er-config db-spec entity-kw fk-id))
      (dissoc m fk-kw))))


(defn- save-dependants!
  "Saves records that point via foreign-key to m."
  [er-config
   db-spec
   m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn query-fn owned?]}]]
  {:pre [(:id m)]}
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
                         (get (if (:id m) :update :insert)))
        saved-m (->> m
                     (without-relations-and-entity er-config entity-kw)
                     (ins-or-up-fn db-spec))]
    (assoc m
      :id (:id saved-m)
      ::entity entity-kw)))


(defn save!
  "Saves an aggregate data structure to the database."
  [er-config db-spec entity-kw m]
  (when m
    ;; first process all records linked with a :one> relation-type
    ;; because we need their ids as foreign keys in m
    (let [relations (-> er-config
                        entity-kw
                        :relations)
          m (->> relations
                 (filter (partial rt? :one>))
                 (filter (fn [[relation-kw relation]]
                           (contains? er-config (:entity-kw relation))))
                 (reduce (partial save-prerequisite! er-config db-spec) m)
                 ;; this will persist m itself (containing all foreign keys)
                 (ins-or-up! er-config db-spec entity-kw))]
      ;; process all other types of relations
      ;; that require m's id as value for the foreign key
      (->> relations
           (remove (partial rt? :one>))
           (reduce (partial save-dependants! er-config db-spec) m)))))
                                       

;;--------------------------------------------------------------------
;; Delete aggregate


(defn- delete-prerequisite!
  "Deletes a record that m points to by a foreign key."
  [er-config db-spec m
   [relation-kw {:keys [relation-type entity-kw fk-kw owned?]}]]
  (when owned?
    (let [fk-id (get m fk-kw)
          child (get m relation-kw)]
      (cond
       child (delete! er-config db-spec entity-kw child)
       fk-id (delete! er-config db-spec entity-kw fk-id)
       :else nil))))


(defn- delete-dependants!
  "Deletes all records that m contains, and that point by foreign key to m."
  [er-config db-spec m
   [relation-kw {:keys [relation-type entity-kw fk-kw update-links-fn owned?]}]]
  (when owned?
    (doseq [child (get m relation-kw)]
      (delete! er-config db-spec entity-kw child)))
  (when (= relation-type :<many>)
    (update-links-fn db-spec (:id m) [])))


(defn delete!
  "Removes an aggregate datastructure from the database."
  [er-config db-spec entity-kw m-or-id]
  (when m-or-id
    (let [delete-fn (-> er-config entity-kw :fns :delete)]
      (if (map? m-or-id)
        (do
          ;; delete all records that might point to m
          (->> er-config entity-kw :relations
               (remove (partial rt? :one>))
               (map (partial delete-dependants! er-config db-spec m-or-id))
               doall)
          ;; delete the record
          (delete-fn db-spec (:id m-or-id))
          ;; delete all owned records that m points to via foreign key
          (->> er-config entity-kw :relations
               (filter (partial rt? :one>))
               (map (partial delete-prerequisite! er-config db-spec m-or-id))
               doall))
        (delete-fn db-spec m-or-id)))))
