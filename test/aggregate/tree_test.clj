(ns aggregate.tree-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))

(use-fixtures :each db-fixture)

(def schema
  [:node [(id-column)
          [:label "varchar(30)"]
          (fk-column :node :parent_id false)]])


(def er
  (agg/make-er-config
   (agg/entity :node
               (agg/->n :children :node {:fk-kw :parent_id})
               (agg/->1 :parent :node))))



;; Tests

(deftest tree-tests
  (create-schema! @db-con schema)
  (testing "Saving and loading a tree of nodes"
    (let [tree {::agg/entity :node
                :label       "a"
                :children    [{:label "aa"}
                              {:label    "ab"
                               :children [{:label "aba"}]}]}]

      (is (= (-> er
                 (agg/without [:node :parent])
                 (agg/save! @db-con tree))
             {::agg/entity :node,
              :label       "a",
              :children
              [{:label "aa", :id 2, ::agg/entity :node}
               {:label       "ab",
                :children    [{:label "aba", :id 4, ::agg/entity :node}],
                :id          3,
                ::agg/entity :node}],
              :id          1}))

      (is (= (-> er
                 (agg/load @db-con :node 1))
             {:id          1,
              :label       "a",
              ::agg/entity :node,
              :children
              [{:id          2, :label "aa",
                ::agg/entity :node,
                :children    []}
               {:id          3,
                :label       "ab",
                ::agg/entity :node,
                :children
                [{:id          4,
                  :label       "aba",
                  ::agg/entity :node,
                  :children    []}]}]}))

      (is (= (-> er
                 (agg/without [:node :children])
                 (agg/load @db-con :node 4))
             {:id          4,
              :label       "aba",
              :parent_id   3,
              ::agg/entity :node,
              :parent
              {:id          3,
               :label       "ab",
               :parent_id   1,
               ::agg/entity :node,
               :parent      {:id          1,
                             :label       "a",
                             ::agg/entity :node}}})))))
