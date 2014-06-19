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
