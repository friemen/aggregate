# aggregate

Handle SQL persistence for nested datastructures.

The name 'aggregate' stems from Eric Evans book
[Domain Driven Design](http://domainlanguage.com/ddd/patterns/).
Martin Fowler briefly explains
[what is meant by the term Aggregate](http://martinfowler.com/bliki/DDD_Aggregate.html).

[![Build Status](https://travis-ci.org/friemen/aggregate.png?branch=master)](https://travis-ci.org/friemen/aggregate)

The library is currently work in progress and not yet released!

## Motivation

Have you ever tried to persist a data graph like this
```clojure
(def project
  {:name "Learning Clojure"
   :tasks [{:desc "Buy a book"}
           {:desc "Install Java"}
           {:desc "Install Emacs"}
           {:desc "Hack!"}]
   :manager {:name "Daisy"})
```
to tables of a relational DB?

Not a big deal... unless you have to write similar DB access code over
and over again for your whole set of domain objects. This library will
do the hard work for you after you have provided some meta data about
the relations among database tables.

Besides being helpful for persisting complex data in relational tables
it is meant to be composable with SQL-oriented libraries like
[Yesql](https://github.com/krisajenkins/yesql),
[HoneySQL](https://github.com/jkk/honeysql) or plain
[clojure.java.jdbc](https://github.com/clojure/java.jdbc).

## Usage

Include this in your namespace declaration:
```clojure
[aggregate.core :as agg]
```


The library mainly provides three functions

* `(load er-config db-spec entity-keyword id)`
* `(save! er-config db-spec entity-keyword data)`
* `(delete! er-config db-spec entity-keyword data)`


The `er-config` describes *relations* between *entities*, and contains
functions that actually read, insert, update or delete records.

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
;;
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
access functions based on core.java.jdbc. But you can use any other
functions with the same behaviour.


How save! and load are used:
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



## License

Copyright © 2014 F.Riemenschneider

Distributed under the Eclipse Public License 1.0.
