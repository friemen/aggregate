# aggregate

Handle SQL persistence for nested datastructures.

Mainly provides three functions

* `(load er-config db-spec entity-keyword id)`
* `(save! er-config db-spec entity-keyword data)`
* `(delete! er-config db-spec entity-keyword data)`

Besides being helpful for persisting complex data in relational tables
it is meant to be composable with SQL-oriented libraries like
[Yesql](https://github.com/krisajenkins/yesql),
[HoneySQL](https://github.com/jkk/honeysql) or direct usage of
[clojure.java.jdbc](https://github.com/clojure/java.jdbc).

The `er-config` describes *entities* and their *relations* to other entities.
It must contain functions that acutally read, insert, update or delete records.

Here's an example
```clojure
;; A database schema with a n <-> m relationship
(def <many>-schema
  {:project [(id-column)
             [:name "varchar(30)"]]
   :person [(id-column)
            [:name "varchar(30)"]]
   :project_person [[:person_id "integer"]
                    [:project_id "integer"]]})
(require '[aggregate.core :as agg])
;= nil
;; A (currently verbose) er-config that enables
;; load, save! and delete! to take the relationship into account
(def <many>-er
  {:project {:fns (agg/make-entity-fns :project)
             :relations {:members {:relation-type :<many>
                                   :entity-kw :person
                                   :query-fn (agg/make-query-<many>-fn :person
                                                                       :project_person
                                                                       :person_id :project_id)
                                   :update-links-fn (agg/make-update-links-fn :project_person
                                                                              :project_id :person_id)
                                   :owned? false}}}
   :person {:fns (agg/make-entity-fns :person)
            :relations {:projects {:relation-type :<many>
                                   :entity-kw :project
                                   :query-fn (agg/make-query-<many>-fn :project
                                                                       :project_person
                                                                       :project_id :person_id)
                                   :update-links-fn (agg/make-update-links-fn :project_person
                                                                              :person_id :project_id)
                                   :owned? false}}}})
```

You can see that the library provides factories that create default DB
access functions based on core.java.jdbc.

An example of usage:
```clojure
(agg/save! <many>-er @db-con :project
            {:name "Webapp"
             :members [{:name "Donald"}
                       {:name "Mickey"}]})
;= {::agg/entity :project
;   :id 1
;   :name "Webapp"
;   :members [{::agg/entity :person
;              :id 1
;              :name "Donald"}
;             {::agg/entity :person
;              :id 2
;              :name "Mickey"}]}
(agg/load <many>-er @db-con :project 1)
;= {::agg/entity :project
;   :id 1
;   :name "Webapp"
;   :members [{::agg/entity :person
;              :id 1
;              :name "Donald"}
;             {::agg/entity :person
;              :id 2
;              :name "Mickey"}]}
```

This is currently work in progress!



## License

Copyright © 2014 F.Riemenschneider

Distributed under the Eclipse Public License 1.0.
