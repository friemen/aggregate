(ns aggregate.scenarios-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))

(use-fixtures :each db-fixture)


;; -------------------------------------------------------------------
;; Scenario 1
;; A ->n B ->1 C, all owned, non-:id pk

(def schema1
  [:a [(id-column "pk")
       [:name "varchar(30)"]]
   :c [(id-column "pk")
       [:name "varchar(30)"]]
   :b [(id-column "pk")
       [:name "varchar(30)"]
       [:a_id "integer references a(pk)"]
       [:c_id "integer references c(pk)"]]])


(def er1
  (agg/make-er-config {:id-kw :pk}
   (agg/entity :a
               (agg/->n :bs :b))
   (agg/entity :b
               (agg/->1 :c :c))
   (agg/entity :c)))


(deftest scenario1-test
  (create-schema! @db-con schema1)
  (testing "Insert all"
    (agg/save! er1 @db-con :a {:name "A"
                               :bs [{:name "B1"
                                     :c {:name "C1"}}
                                    {:name "B2"
                                     :c {:name "C2"}}]})
    (is (= 1 (record-count @db-con :a)))
    (is (= 2 (record-count @db-con :b)))
    (is (= 2 (record-count @db-con :c))))
  (testing "Delete a B directly"
    (let [a (agg/load er1 @db-con :a 1)
          b1 (-> a :bs (get 0))]
      (is (= {:bs
              [{:c {:aggregate.core/entity :c, :name "C1", :pk 1},
                :aggregate.core/entity :b,
                :c_id 1,
                :a_id 1,
                :name "B1",
                :pk 1}
               {:c {:aggregate.core/entity :c, :name "C2", :pk 2},
                :aggregate.core/entity :b,
                :c_id 2,
                :a_id 1,
                :name "B2",
                :pk 2}],
              :aggregate.core/entity :a,
              :name "A",
              :pk 1}
             a))
      (is (= 2 (agg/delete! er1 @db-con b1)))))
  (testing "Remove a B from A then save"
    (let [a (agg/load er1 @db-con :a 1)
          a-without-b2 (update-in a [:bs] butlast)]
      (agg/save! er1 @db-con a-without-b2)
      (is (= {:bs [],
              :aggregate.core/entity :a,
              :name "A",
              :pk 1}
             (agg/load er1 @db-con :a 1)))
      (is (= 0 (record-count @db-con :b)))
      (is (= 0 (record-count @db-con :c)))
      (is (= 1 (agg/delete! er1 @db-con a)))
      (is (= 0 (record-count @db-con :a)))))
  (testing "Insert an A, then remove everything"
    (let [saved-a (agg/save! er1 @db-con :a {:name "A"
                                             :bs [{:name "B1"}
                                                  {:name "B2"
                                                   :c {:name "C1"}}]})]
      (is (= 1 (record-count @db-con :a)))
      (is (= 2 (record-count @db-con :b)))
      (is (= 1 (record-count @db-con :c)))
      (is (= 4 (agg/delete! er1 @db-con saved-a)))))
  (testing "Insert an A, then remove a C from a B"
    (let [saved-a (agg/save! er1 @db-con :a {:name "A"
                                             :bs [{:name "B1"
                                                   :c {:name "C1"}}]})]
      (is (= 1 (record-count @db-con :c)))
      (agg/save! er1 @db-con (update-in saved-a [:bs 0] dissoc :c))
      (is (= 1 (record-count @db-con :b)))
      (is (= 0 (record-count @db-con :c))))))



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
  

;; -------------------------------------------------------------------
;; Scenario 3
;; A ->mn B, B owns A, non-:id keys


(def schema3
  [:a [(id-column :ad)
       [:name "varchar(30)"]]
   :b [(id-column :bd)
       [:name "varchar(30)"]]
   :a_b [[:a_id "integer references a(ad)"]
         [:b_id "integer references b(bd)"]]])

(def er3
  (agg/make-er-config
   (agg/entity :a
               {:id-kw :ad}
               (agg/->mn :bs :b {:owned? true
                                 :query-fn (agg/make-query-<many>-fn :b :a_b :a_id :b_id :bd)}))
   (agg/entity :b
               {:id-kw :bd}
               (agg/->mn :as :a {:query-fn (agg/make-query-<many>-fn :a :a_b :b_id :a_id :ad)
                                 :update-links-fn (agg/make-update-links-fn :a_b :b_id :a_id :ad)
                                 :owned? true}))))


(deftest scenario3-test
  (create-schema! @db-con schema3)
  (let [saved-as (->> (range 1 11)
                      (map #(hash-map :name (str "A" %)))
                      (map (partial agg/save! er3 @db-con :a))
                      doall)
        saved-bs (->> (range 1 11)
                      (map #(hash-map :name (str "B" %)))
                      (map (partial agg/save! er3 @db-con :b))
                      doall)]
    (is (= 10 (record-count @db-con :a)))
    (is (= 10 (record-count @db-con :b)))
    (is (= 0 (record-count @db-con :a_b)))
    (testing "Link first A with first 5 Bs"
      (let [a1 (assoc (first saved-as) :bs (take 5 saved-bs))]
        (is (= 0 (->> a1 :ad (agg/load er3 @db-con :a) :bs count)))
        (agg/save! er3 @db-con a1)
        (is (= 10 (record-count @db-con :a)))
        (is (= 10 (record-count @db-con :b)))
        (is (= 5 (record-count @db-con :a_b)))
        (is (= 5 (->> a1 :ad (agg/load er3 @db-con :a) :bs count)))))
    (testing "Load first 5 Bs, assert all point to A1"
      (doseq  [b (->> [1 2 3 4 5] (map (partial agg/load er3 @db-con :b)))]
        (is (= "A1" (-> b :as first :name)))
        (is (= 1 (-> b :as count)))))
    (testing "Delete all Bs pointing to A1, assert that 9 As remain"
      (->> [1 2 3 4 5]
           (map (partial agg/load er3 @db-con :b))
           (map (partial agg/delete! er3 @db-con))
           doall)
      (is (= 9 (record-count @db-con :a)))
      (is (= 5 (record-count @db-con :b)))
      (is (= 0 (record-count @db-con :a_b))))))
