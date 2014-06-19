(ns aggregate.scenarios-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))

(use-fixtures :each db-fixture)


;; -------------------------------------------------------------------
;; Scenario 1
;; A ->n B ->1 C

(def schema1
  [:a [(id-column)
       [:name "varchar(30)"]]
   :c [(id-column)
       [:name "varchar(30)"]]
   :b [(id-column)
       [:name "varchar(30)"]
       (fk-column :a false)
       (fk-column :c true)]])


(def er1
  (agg/make-er-config
   (agg/entity :a {}
               (agg/->n :bs :b))
   (agg/entity :b {}
               (agg/->1 :c :c))
   (agg/entity :c)))


(deftest scenario1-test
  (create-schema! @db-con schema1)
  (agg/save! er1 @db-con :a {:name "A"
                             :bs [{:name "B1"
                                   :c {:name "C1"}}
                                  {:name "B2"
                                   :c {:name "C2"}}]})
  (is (= 1 (record-count @db-con :a)))
  (is (= 2 (record-count @db-con :b)))
  (is (= 2 (record-count @db-con :c)))
  (let [a (agg/load er1 @db-con :a 1)
        b1 (-> a :bs (get 0))]
    (is (= {:bs
            [{:c {:aggregate.core/entity :c, :name "C1", :id 1},
              :aggregate.core/entity :b,
              :c_id 1,
              :a_id 1,
              :name "B1",
              :id 1}
             {:c {:aggregate.core/entity :c, :name "C2", :id 2},
              :aggregate.core/entity :b,
              :c_id 2,
              :a_id 1,
              :name "B2",
              :id 2}],
            :aggregate.core/entity :a,
            :name "A",
            :id 1}
           a))
    (is (= 2 (agg/delete! er1 @db-con b1))))
  (let [a (agg/load er1 @db-con :a 1)
        a-without-b2 (update-in a [:bs] butlast)]
    (agg/save! er1 @db-con a-without-b2)
    (is (= {:bs [],
            :aggregate.core/entity :a,
            :name "A",
            :id 1}
           (agg/load er1 @db-con :a 1)))
    (is (= 0 (record-count @db-con :b)))
    (is (= 0 (record-count @db-con :c)))
    (is (= 1 (agg/delete! er1 @db-con a)))
    (is (= 0 (record-count @db-con :a)))))



;; -------------------------------------------------------------------
;; Scenario 2
;; A ->1 B ->mn C, none owned


(def schema2
  [:b [(id-column)
       [:name "varchar(30)"]]
   :a [(id-column)
       [:name "varchar(30)"]
       (fk-column :b false)]
   :c [(id-column)
       [:name "varchar(30)"]]
   :b_c [(fk-column :b false)
         (fk-column :c false)]])

(def er2
  (agg/make-er-config
   (agg/entity :a {}
               (agg/->1 :b :b {:owned? false}))
   (agg/entity :b {}
               (agg/->mn :cs :c))
   (agg/entity :c {}
               (agg/->mn :bs :b {:query-fn (agg/make-query-<many>-fn :b :b_c :c_id :b_id)
                                 :update-links-fn (agg/make-update-links-fn :b_c :c_id :b_id)}))))


(deftest scenario2-test
  (create-schema! @db-con schema2)
  (let [saved-a (agg/save! er2 @db-con :a {:name "A"
                                           :b {:name "B1"
                                               :cs [{:name "C1"}
                                                    {:name "C2"}]}})]
    (testing "Did everything arrive in DB?"
      (is (= 1 (record-count @db-con :a)))
      (is (= 1 (record-count @db-con :b)))
      (is (= 2 (record-count @db-con :c)))
      (is (= 2 (record-count @db-con :b_c))))
    (testing "Delete A, but Bs and Cs must survive"
      (agg/delete! er2 @db-con saved-a)  
      (is (= 0 (record-count @db-con :a)))
      (is (= 1 (record-count @db-con :b)))
      (is (= 2 (record-count @db-con :c)))
      (is (= 2 (record-count @db-con :b_c))))
    (testing "Delete B, but Cs must survive"
      (agg/delete! er2 @db-con (-> saved-a :b))
      (is (= 0 (record-count @db-con :b)))
      (is (= 2 (record-count @db-con :c)))
      (is (= 0 (record-count @db-con :b_c)))))
  (let [saved-a (agg/save! er2 @db-con :a {:name "A"
                                           :b {:name "B2"
                                               :cs [{:id 1 :name "C1"}
                                                    {:id 2 :name "C2"}]}})]
    (testing "Add another C within A"
      (agg/save! er2 @db-con (-> saved-a :b (update-in [:cs] conj {:name "C3"})))
      (is (= 3 (record-count @db-con :c)))
      (is (= 3 (record-count @db-con :b_c))))
    (testing "Add another B, B3 and B2 share C3"
      (agg/save! er2 @db-con :b {:name "B3"
                                 :cs [{:id 3 :name "C3"}
                                      {:name "C4"}]})
      (is (= 4 (record-count @db-con :c)))
      (is (= 5 (record-count @db-con :b_c)))
      (is (= 3 (-> (agg/load er2 @db-con :b 2) :cs count)))
      (is (= 2 (-> (agg/load er2 @db-con :b 3) :cs count)))
      (is (= 2 (-> (agg/load er2 @db-con :c 3) :bs count))))))
  
