(ns aggregate.project-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))


(use-fixtures :each db-fixture)

(def schema
  [:customer [(id-column)
              [:name "varchar(30)"]]
   :person [(id-column)
            [:name "varchar(30)"]]
   :project [(id-column)
             [:name "varchar(30)"]
             (fk-column :person :manager_id false)
             (fk-column :customer false)]
   :task [(id-column)
          [:description "varchar(50)"]
          [:effort "integer"]
          (fk-column :project false)
          (fk-column :person :assignee_id false)]
   :person_project [(fk-column :project false)
                    (fk-column :person false)]])



(def er
  "The complete entity-relationship model.
  Be careful, using this directly may take the complete database into account."
  (agg/make-er-config
   (agg/entity :customer
               (agg/->n :projects :project {:fk-kw :customer_id}))
   (agg/entity :person
               (agg/->n :tasks :task {:fk-kw :assignee_id
                                      :owned? false})
               (agg/->n :projects_as_manager :project {:fk-kw :manager_id
                                                       :owned? false})
               (agg/->mn :projects_as_member :project))
   (agg/entity :project
               (agg/->1 :customer :customer {:owned? false})
               (agg/->mn :members :person {:query-fn (agg/make-query-<many>-fn
                                                      :person
                                                      :person_project
                                                      :project_id
                                                      :person_id)
                                           :update-links-fn (agg/make-update-links-fn
                                                             :person_project
                                                             :project_id
                                                             :person_id
                                                             :id)})
               (agg/->1 :manager :person {:owned? false})
               (agg/->n :tasks :task {:fk-kw :project_id}))
   (agg/entity :task
               (agg/->1 :project :project {:owned? false})
               (agg/->1 :assignee :person {:owned? false}))))





(def manage-person-to-project-er
  (-> er (agg/only [:person]
                   [:project :members :manager])))

(def manage-task-to-person-er
  "Supports task-to-person assignment, cutting off project related links."
  (-> er (agg/without [:task :project]
                      [:person :projects_as_member :projects_as_manager])))


;; To setup a schema in standalone H2
#_ (do (require '[aggregate.h2 :as h2])
       (require '[aggregate.testsupport :refer :all])
       (require '[aggregate.core :as agg])
       (h2/start-db))

#_ (create-schema! @h2/db-con schema)


(def data [[:person {:name "Daisy"}]
           [:person {:name "Mini"}]
           [:person {:name "Mickey"}]
           [:person {:name "Donald"}]
           [:customer {:name "Big Company"}]
           [:customer {:name "Startup"}]])

(deftest project-tests
  (create-schema! @db-con schema)
  (->> data (map (partial apply agg/save! er @db-con)) doall)
  (testing "Creating a new project"
    (let [saved-project (agg/save! er @db-con :project
                                   {:name "Learning Clojure"
                                    :customer {:id 1 :name "Big Company"}
                                    :tasks [{:description "Buy a good book" :effort 1}
                                            {:description "Install Java" :effort 2}
                                            {:description "Configure Emacs" :effort 4}]
                                    :members [{:id 1 :name "Daisy"}
                                              {:id 2 :name "Mini"}]
                                    :manager {:id 1 :name "Daisy"}})]
      (is (= 1 (record-count @db-con :project)))
      (testing "Assign persons to projects"
        (->> (agg/load manage-person-to-project-er @db-con :project 1)
             (#(update-in % [:members] conj {:id 3 :name "Mickey"}))
             (agg/save! manage-person-to-project-er @db-con)))
      (testing "Assign person to task"
        (->> (agg/load manage-task-to-person-er @db-con :task 1)
             (#(assoc % :assignee {:id 2 :name "Mini"}))
             (agg/save! manage-task-to-person-er @db-con :task))
        (let [loaded-project (agg/load er @db-con :project 1) 
              loaded-daisy (agg/load er @db-con :person 1)
              loaded-mini (agg/load er @db-con :person 2)]
          (is (-> loaded-project :customer))
          (is (= 3 (-> loaded-project :members count)))
          (is (= 1 (-> loaded-daisy :projects_as_member count)))
          (is (= 1 (-> loaded-daisy :projects_as_manager count)))
          (is (= 0 (-> loaded-daisy :tasks count)))
          (is (= 1 (-> loaded-mini :projects_as_member count)))
          (is (= 0 (-> loaded-mini :projects_as_manager count)))
          (is (= 1 (-> loaded-mini :tasks count)))))
      (testing "Delete a person that a task points to"
        (is (-> (agg/load manage-task-to-person-er @db-con :task 1) :assignee))
        (agg/delete! er @db-con (agg/load er @db-con :person 2))
        (is (nil? (-> (agg/load manage-task-to-person-er @db-con :task 1) :assignee))))
      (testing "Delete the project"
        (agg/delete! er @db-con saved-project)))))


