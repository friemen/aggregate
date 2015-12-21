(ns aggregate.betting-test
  (:require [clojure.test :refer :all]
            [aggregate.core :as agg]
            [aggregate.testsupport :refer :all]
            [clojure.java.jdbc :as jdbc]))

(use-fixtures :each db-fixture)

(def schema
  [:gambler [(id-column)
             [:name "varchar(30)"]]
   :team [(id-column)
          [:name "varchar(30)"]]
   :match [(id-column)
           (fk-column :team :team_a_id false)
           (fk-column :team :team_b_id false)
           [:result "varchar(10)"]]
   :bet [(id-column)
         (fk-column :gambler false)
         (fk-column :match false)
         [:tip "varchar(10)"]]])


(def er
  (agg/make-er-config
   (agg/entity :gambler
               (agg/->n :bets :bet {:fk-kw :gambler_id}))
   (agg/entity :match
               (agg/->1 :team_a :team {:owned? false})
               (agg/->1 :team_b :team {:owned? false})
               (agg/->n :bets :bet {:fk-kw :match_id}))
   (agg/entity :team
               (agg/->n :matches_as_a :match {:fk-kw :team_a_id})
               (agg/->n :matches_as_b :match {:fk-kw :team_b_id}))
   (agg/entity :bet
               (agg/->1 :gambler :gambler {:owned? false})
               (agg/->1 :match :match {:owned? false}))))


(defn dump-match
  [con]
  (dump-table con :match [[:id 3] [:result 10] [:team_a_id 10] [:team_b_id 10]]))

(defn dump-team
  [con]
  (dump-table con :team [[:id 3] [:name 10]]))

(defn dump-gambler
  [con]
  (dump-table con :gambler [[:id 3] [:name 10]]))

(defn dump-bet
  [con]
  (dump-table con :bet [[:id 3] [:tip 10] [:gambler_id 10] [:match_id 10]]))

;; To setup a schema in standalone H2
#_ (do (require '[aggregate.h2 :as h2])
       (h2/start-db))

#_ (create-schema! @h2/db-con schema)

;; Tests

(deftest betting-tests
  (create-schema! @db-con schema)
  (testing "Bet links gambler and match"
    (->> [[:gambler {:name "Donald"}]
          [:gambler {:name "Daisy"}]
          [:match {:team_a {:name "Germany"}
                   :team_b {:name "Portugal"}
                   :result "4:0"}]
          [:bet {:tip "2:0" :gambler_id 1 :match_id 1}]
          [:bet {:tip "1:1" :gambler_id 2 :match_id 1}]]
         (map (partial apply agg/save! er @db-con))
         doall)
    (let [loaded-match (agg/load er @db-con :match 1)]
      (is (= 2 (-> loaded-match :bets count))))
    (let [loaded-team (agg/load er @db-con :team 1)]
      (is (= 1 (-> loaded-team :matches_as_a count)))
      (is (= 0 (-> loaded-team :matches_as_b count))))
    (let [loaded-bet (agg/load er @db-con :bet 1)]
      (is (= "Germany" (-> loaded-bet :match :team_a :name)))
      (is (= "Portugal" (-> loaded-bet :match :team_b :name)))
      (is (= "Donald" (-> loaded-bet :gambler :name)))
      (is (= "4:0" (-> loaded-bet :match :result)))
      (testing "Remove a gambler"
        (agg/save! er @db-con :bet (assoc loaded-bet :gambler nil))
        (is (= 2 (record-count @db-con :gambler)))
        (is (nil? (-> (agg/load er @db-con :bet 1) :gambler))))
      (testing "Add a link to another, existing gambler"
        (agg/save! er @db-con :bet (assoc loaded-bet :gambler {:id 2 :name "Daisy"}))
        (is (= 2 (record-count @db-con :gambler)))
        (is (= "Daisy" (-> (agg/load er @db-con :bet 1) :gambler :name)))
        (is (= 2 (-> (agg/load er @db-con :gambler 2) :bets count))))
      (testing "Add a match with one new and one existing team"
        (agg/save! er @db-con :match {:result "unknown"
                                      :team_a {:id 1 :name "Germany"}
                                      :team_b {:name "USA"}})
        (is (= 3 (record-count @db-con :team)))
        (is (= 2 (-> (agg/load er @db-con :team 1) :matches_as_a count))))
      (testing "Bets can be deleted without affecting matches or gamblers"
        (agg/delete! er @db-con :bet (agg/load er @db-con :bet 1))
        (is (= 2 (record-count @db-con :gambler)))
        (is (= 2 (record-count @db-con :match)))
        (is (= 1 (record-count @db-con :bet)))))))
