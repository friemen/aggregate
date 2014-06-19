(ns aggregate.core-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))

(use-fixtures :each db-fixture)


;; -------------------------------------------------------------------
;; Misc tests


(deftest without-test
  (let [er {:project {:relations {:tasks {}
                                  :members {}
                                  :customer {}}}
            :task {:relations {:project {}}}}]
    (is (= {:task {:relations {:project {}}}}
           (-> er (agg/without :project))))
    (is (= {:project {:relations {:tasks {}}}}
           (-> er (agg/without [:project :members :customer] :task))))))


(deftest only-test
  (let [er {:project {:relations {:tasks {}
                                  :members {}
                                  :customer {}}}
            :task {:relations {:project {}}}}]
    (is (= {:project {:relations {}}
            :task {:relations {:project {}}}}
           (-> er (agg/only [:project]))))
    (is (= {:project {:relations {:members {}
                                  :customer {}}}
            :task {:relations {}}}
           (-> er (agg/only [:project :members :customer]
                            [:task]))))))


(deftest replace-default-factory-test
  (let [er (agg/make-er-config {:read-fn-factory (constantly "bar")
                                :query-<many-fn-factory (constantly "foo")}
                               (agg/entity :a
                                           (agg/->n :bs :b))
                               (agg/entity :b {:fns {:read "bum"}}
                                           (agg/->n :as :a {:query-fn "baz"})))]
    (is (= "bar" (-> er :a :fns :read)))
    (is (= "foo" (-> er :a :relations :bs :query-fn)))
    (is (= "bum" (-> er :b :fns :read)))
    (is (= "baz" (-> er :b :relations :as :query-fn)))))


;; -------------------------------------------------------------------
;; Tests on the simple schema

(def minimal-schema
  [:person     [(id-column)
                [:name "varchar(30)"]]])

(defn setup-minimal!
  [con]
  (create-schema! @db-con minimal-schema)
  (jdbc/insert! con :person {:name "Foo"}))



(deftest read-fn-test
  (setup-minimal! @db-con)
  (is (= {:id 1 :name "Foo"} ((agg/make-read-fn "person") @db-con 1)))
  (is (= {:id 1 :name "Foo"} ((agg/make-read-fn :person) @db-con 1)))
  (is (= nil ((agg/make-read-fn :person) @db-con 2))))


(deftest insert-fn-test
  (setup-minimal! @db-con)
  (let [result ((agg/make-insert-fn "person") @db-con {:name "Bar"})]
    ; :id is part of the record
    (is (= {:id 2 :name "Bar"} result))
    ; a record has actually been added
    (is (= 2 (record-count @db-con "person")))
    ; the new record can be found by its id
    (is (= {:id 2 :name "Bar"} ((agg/make-read-fn :person) @db-con 2)))))


(deftest update-fn-test
  (setup-minimal! @db-con)
  (let [result ((agg/make-update-fn :person) @db-con {:id 1 :name "Baz"})]
    ; the set-map is returned
    (is (= {:id 1 :name "Baz"} result))
    (is (= {:id 1 :name "Baz"} ((agg/make-read-fn :person) @db-con 1)))
    (is (= 1 (record-count @db-con :person)))
    (is (thrown? AssertionError ((agg/make-update-fn :person) @db-con {:name "Baz"}) ))))


(deftest delete-fn-test
  (setup-minimal! @db-con)
  (is (= 1 (record-count @db-con "person")))
  (is ((agg/make-delete-fn :person) @db-con 1))
  (is (= 0 (record-count @db-con "person")))
  (is (not ((agg/make-delete-fn :person) @db-con 1))))


(def minimal-er
  {:person {:fns (agg/make-entity-fns :person)}})


(deftest save-load-minimal-test
  (setup-minimal! @db-con)
  (let [m {:name "Bar"}
        expected {::agg/entity :person
                  :id 2
                  :name "Bar"}]
    (is (= nil (agg/load minimal-er @db-con :person 2)))
    (is (= expected (agg/save! minimal-er @db-con :person m)))
    (is (= expected (agg/load minimal-er @db-con :person 2)))
    (is (= nil (agg/load minimal-er @db-con :person 3)))))


(deftest save-existing-minimal-test
  (setup-minimal! @db-con)
  (let [m {:name "Bar"}
        saved (agg/save! minimal-er @db-con :person m)
        updated (assoc saved :name "Baz")
        expected {::agg/entity :person
                  :id 2
                  :name "Baz"}]
    (agg/save! minimal-er @db-con :person updated)
    (is (= expected (agg/load minimal-er @db-con :person 2)))))


(deftest delete-minimal-test
  (setup-minimal! @db-con)
  (testing "Delete record by id"
    (agg/delete! minimal-er @db-con :person 1)
    (is (= 0 (record-count @db-con :person))))
  (testing "Delete loaded record"
    (let [m {:name "Foo"}
          saved-m (agg/save! minimal-er @db-con :person m)]
      (is (= 1 (record-count @db-con :person)))
      (is (= 1 (agg/delete! minimal-er @db-con :person saved-m)))
      (is (= 0 (record-count @db-con :person))))))


;; -------------------------------------------------------------------
;; Tests with a :one> (to 1) relationship

(def one>-schema
  [:address [(id-column)
             [:street "varchar(20)"]]
   :person [(id-column)
            [:name "varchar(30)"]
            (fk-column :address false)]])

(def one>-er
  {:person {:fns (agg/make-entity-fns :person)
            :relations {:address {:relation-type :one>
                                  :entity-kw :address
                                  :fk-kw :address_id
                                  :owned? true}}}
   :address {:fns (agg/make-entity-fns :address)}})


(defn setup-one>!
  [con]
  (create-schema! con one>-schema))


(deftest save-load-one>-test
  (setup-one>! @db-con)
  (let [m {:name "Foo"
           :address {:street "Barstreet"}}
        expected {::agg/entity :person
                  :id 1
                  :name "Foo"
                  :address_id 1
                  :address {::agg/entity :address
                            :id 1
                            :street "Barstreet"}}]
    (is (= expected (agg/save! one>-er @db-con :person m)))
    (is (= expected (agg/load one>-er @db-con :person 1)))
    (is (= nil (agg/load one>-er @db-con :person 2)))))


(deftest add-one>-test
  (setup-one>! @db-con)
  (let [m {:name "Foo"}
        expected {::agg/entity :person
                  :id 1
                  :name "Foo"
                  :address_id 1
                  :address {::agg/entity :address
                            :id 1
                            :street "Barstreet"}}
        saved-m (agg/save! one>-er @db-con :person m)]
    (is (not (contains? saved-m :address)))
    (is (not (contains? (agg/load one>-er @db-con :person 1) :address)))
    (is (= expected (agg/save! one>-er @db-con :person (assoc saved-m :address {:street "Barstreet"}))))
    (is (= expected (agg/load one>-er @db-con :person 1)))))


(deftest remove-one>-test
  (setup-one>! @db-con)
  (let [m {:name "Foo"
           :address {:street "Barstreet"}}
        expected {::agg/entity :person
                  :id 1
                  :name "Foo"}
        saved-m (agg/save! one>-er @db-con :person m)]
    (is (= 1 (record-count @db-con :address)))
    (is (= expected (agg/save! one>-er @db-con :person (dissoc saved-m :address))))
    (is (= expected (agg/load one>-er @db-con :person 1)))
    (is (= 0 (record-count @db-con :address)))))


(deftest delete-one>-test
  (setup-one>! @db-con)
  (let [m {:name "Foo"
           :address {:street "Barstreet"}}
        saved-m (agg/save! one>-er @db-con :person m)]
    (is (= 1 (record-count @db-con :person)))
    (is (= 1 (record-count @db-con :address)))
    (is (= 2 (agg/delete! one>-er @db-con :person saved-m)))
    (is (= 0 (record-count @db-con :person)))
    (is (= 0 (record-count @db-con :address)))))


;; -------------------------------------------------------------------
;; Tests with a :<many (1 to n) relationship

(def <many-schema
  [:project [(id-column)
               [:name "varchar(30)"]]
   :task [(id-column)
            [:desc "varchar(30)"]
            (fk-column :project true)]])

(def <many-er
  {:project {:fns (agg/make-entity-fns :project)
             :relations {:tasks {:relation-type :<many
                                 :entity-kw :task
                                 :fk-kw :project_id
                                 :query-fn (agg/make-query-<many-fn "task" :project_id)
                                 :owned? true}}}
   :task {:fns (agg/make-entity-fns :task)
          :relations {:project {:relation-type :one>
                                :entity-kw :project
                                :fk-kw :project_id
                                :owned? false}}}})

(defn setup-<many!
  [con]
  (create-schema! con <many-schema))

(defn make-task
  [id desc]
  {::agg/entity :task :id id :desc desc})


(def project {:name "Learning Clojure"
              :tasks [{:desc "Buy a book"}
                      {:desc "Install Java"}
                      {:desc "Install Emacs"}
                      {:desc "Hack!"}]})

(deftest save-load-<many-test
  (setup-<many! @db-con)
  (testing "Project with tasks"
    (let [m project
          expected {::agg/entity :project
                    :id 1
                    :name "Learning Clojure"
                    :tasks (mapv (partial apply make-task) [[1 "Buy a book"]
                                                            [2 "Install Java"]
                                                            [3 "Install Emacs"]
                                                            [4 "Hack!"]])}]
      (is (= expected (agg/save! <many-er @db-con :project m)))
      (is (= expected (agg/load <many-er @db-con :project 1)))
      (testing "Saving the returned data again has no effect"    
        (is (= expected (agg/save! <many-er @db-con :project expected)))
        (is (= expected (agg/load <many-er @db-con :project 1))))))
  (testing "Task with its project"
    (let [expected {::agg/entity :task
                    :id 1
                    :desc "Buy a book"
                    :project_id 1
                    :project {::agg/entity :project
                              :id 1
                              :name "Learning Clojure"}}]
      (is (= expected (agg/load <many-er @db-con :task 1))))))


(deftest add-<many-item-test
  (setup-<many! @db-con)
  (testing "Add an item to an already saved project"
    (let [m (agg/save! <many-er @db-con :project project)
          expected (update-in m [:tasks] conj (make-task 5 "Start REPL"))]
      (is (= expected (agg/save! <many-er @db-con :project (update-in m [:tasks] conj {:desc "Start REPL"}))))
      (is (= expected (agg/load <many-er @db-con :project 1))))))


(deftest remove-<many-item-test
  (setup-<many! @db-con)
  (testing "Remove a task from a saved project"
    (let [m (agg/save! <many-er @db-con :project project)
          expected (update-in m [:tasks] (comp vec butlast))]
      (is (= expected (agg/save! <many-er @db-con :project (update-in m [:tasks] (comp vec butlast)))))
      (is (= expected (agg/load <many-er @db-con :project 1)))))
  (testing "Remove all tasks from a saved project"
    (let [m (agg/load <many-er @db-con :project 1)
          expected (assoc m :tasks [])]
      (is (= expected (agg/save! <many-er @db-con :project (assoc m :tasks []))))
      (is (= expected (agg/load <many-er @db-con :project 1))))))


(deftest delete-<many-test
  (setup-<many! @db-con)
  (let [m project
        saved-m (agg/save! <many-er @db-con :project m)]
    (is (= 1 (record-count @db-con :project)))
    (is (= 4 (record-count @db-con :task)))
    (is (= 5 (agg/delete! <many-er @db-con :project saved-m)))
    (is (= 0 (record-count @db-con :project)))
    (is (= 0 (record-count @db-con :task)))))


;; -------------------------------------------------------------------
;; Tests with a :<many> (m to n) relationship

(def <many>-schema
  [:project [(id-column)
             [:name "varchar(30)"]]
   :person [(id-column)
            [:name "varchar(30)"]]
   :project_person [(fk-column :person false)
                    (fk-column :project false)]])


(def <many>-er
  {:project {:fns (agg/make-entity-fns :project)
             :relations {:members {:relation-type :<many>
                                   :entity-kw :person
                                   :query-fn (agg/make-query-<many>-fn :person
                                                                       :project_person
                                                                       :project_id :person_id)
                                   :update-links-fn (agg/make-update-links-fn :project_person
                                                                              :project_id :person_id)
                                   :owned? false}}}
   :person {:fns (agg/make-entity-fns :person)
            :relations {:projects {:relation-type :<many>
                                   :entity-kw :project
                                   :query-fn (agg/make-query-<many>-fn :project
                                                                       :project_person
                                                                       :person_id :project_id)
                                   :update-links-fn (agg/make-update-links-fn :project_person
                                                                              :person_id :project_id)
                                   :owned? false}}}})

(defn setup-<many>!
  [con]
  (create-schema! con <many>-schema))


(deftest save-load-<many>-test
  (setup-<many>! @db-con)
  (testing "One project with some members"
    (let [m {:name "Webapp"
             :members [{:name "Donald"}
                       {:name "Mickey"}]}
          expected {::agg/entity :project
                    :id 1
                    :name "Webapp"
                    :members [{::agg/entity :person
                               :id 1
                               :name "Donald"}
                              {::agg/entity :person
                               :id 2
                               :name "Mickey"}]}]
      (is (= expected (agg/save! <many>-er @db-con :project m)))
      (is (= 2 (record-count @db-con :project_person)))
      (is (= expected (agg/load <many>-er @db-con :project 1))))))


(deftest add-<many>-item-test
  (setup-<many>! @db-con)
  (testing "Add an item to an existing project"
    (let [m {:name "Webapp"
             :members [{:name "Donald"}
                       {:name "Mickey"}]}
          saved-m (agg/save! <many>-er @db-con :project m)
          expected (update-in saved-m [:members] conj {::agg/entity :person :id 3 :name "Daisy"})]
      (is (= expected (agg/save! <many>-er @db-con :project (update-in saved-m [:members] conj {:name "Daisy"}))))
      (is (= expected (agg/load <many>-er @db-con :project 1))))))


(deftest remove-<many>-link-test
  (setup-<many>! @db-con)
  (testing "Remove the link between person and project"
    (let [m {:name "Webapp"
             :members [{:name "Donald"}
                       {:name "Mickey"}]}
          saved-m (agg/save! <many>-er @db-con :project m)
          expected (update-in saved-m [:members] (comp vec butlast))]
      (is (= expected (agg/save! <many>-er @db-con :project (update-in saved-m [:members] (comp vec butlast)))))
      (is (= 1 (record-count @db-con :project_person)))
      (is (= 2 (record-count @db-con :person))))))


(deftest delete-<many>-test
  (setup-<many>! @db-con)
  (let [m {:name "Webapp"
           :members [{:name "Donald"}
                     {:name "Mickey"}]}
        saved-m (agg/save! <many>-er @db-con :project m)]
    (is (= 1 (record-count @db-con :project)))
    (is (= 2 (record-count @db-con :person)))
    (is (= 2 (record-count @db-con :project_person)))
    (is (= 1 (agg/delete! <many>-er @db-con :project saved-m)))
    (is (= 0 (record-count @db-con :project)))
    (is (= 2 (record-count @db-con :person)))
    (is (= 0 (record-count @db-con :project_person)))))


;; -------------------------------------------------------------------
;; Testing functions to build er-config

(deftest make-er-config-test
  (let [er-config (agg/make-er-config
                   (agg/entity :project
                               {}
                               (agg/->mn :members :person)
                               (agg/->n :tasks :task)
                               (agg/->1 :manager :person))
                   (agg/entity :person))]
    (is (-> er-config :project :fns :read))
    (is (-> er-config :person :fns :delete))
    (is (-> er-config :project :relations :tasks :query-fn))
    (is (-> er-config :project :relations :members :update-links-fn))
    (is (= :manager_id (-> er-config :project :relations :manager :fk-kw)))))



;; -------------------------------------------------------------------
;; Tests with a more realistic schema

(def betting-schema
  [:player [(id-column)
            [:name "varchar(30)"]]
   :bet [(id-column)
         (fk-column :game false)
         (fk-column :gambler false)
         [:predicted_result "varchar(10)"]]
   :gambler [(id-column)
             [:name "varchar(30)"]]
   :game [(id-column)
          (fk-column :player :player_a_id false)
          (fk-column :player :player_b_id false)]])

(def betting-er
  {:player {:fns (agg/make-entity-fns :player)
            :relations {:games {:relation-type :<many>
                                :entity-kw :game
                                :query-fn nil
                                :update-links-fn nil
                                :owned? false}}}
   :bet {:fns (agg/make-entity-fns :bet)
         :relations {:game {:relation-type :<one
                            :entity-kw :game
                            :fk-kw :game_id}
                     :gambler {:relation-type :<one
                               :entity-kw :gambler
                               :fk-kw :gambler_id}}}
   :gambler {:fns (agg/make-entity-fns :gambler)
             :relations {:games {:relation-type :<many>
                                 :entity-kw :game
                                 :query-fn nil
                                 :update-links-fn nil
                                 :owned? false}}}
   :game {:fns (agg/make-entity-fns :game)}})


