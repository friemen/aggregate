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
the relations between database tables.

Besides being helpful for persisting complex data in relational tables
it is meant to be composable with SQL-oriented libraries like
[Yesql](https://github.com/krisajenkins/yesql),
[HoneySQL](https://github.com/jkk/honeysql) or plain
[clojure.java.jdbc](https://github.com/clojure/java.jdbc).

## A first glimpse

Include this in your namespace declaration:
```clojure
[aggregate.core :as agg]
```

Or for testing in the REPL execute
```clojure
(do (require '[aggregate.core :as agg])
    (require '[aggregate.testsupport :refer :all]))
```


The library mainly provides three functions

* `(load er-config db-spec entity-keyword id)`
* `(save! er-config db-spec entity-keyword data)`
* `(delete! er-config db-spec entity-keyword data)`


The `er-config` describes *relations* between *entities*, and contains
functions that actually read, insert, update or delete records.

Here's an example. To ease understanding the following picture gives
an overview of a bunch of tables and their relations:

![Project Example](images/project-example.png)

The corresponding schema definition looks like this
```clojure
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
          [:desc "varchar(50)"]
          [:effort "integer"]
          (fk-column :project false)
          (fk-column :person :assignee_id false)]
   :person_project [(fk-column :project false)
                    (fk-column :person false)]])
```

What follows is a global er-config that enables load, save! and delete!
to take the relationships into account.

```clojure
(def er
  "The complete entity-relationship model.
  Be careful, using this directly may take the complete database into account."
  (agg/make-er-config
   (agg/entity :customer {}
               (agg/->n :projects :project {:fk-kw :customer_id}))
   (agg/entity :person {}
               (agg/->n :tasks :task {:fk-kw :assignee_id
                                      :owned? false})
               (agg/->n :projects_as_manager :project {:fk-kw :manager_id
                                                       :owned? false})
               (agg/->mn :projects_as_member :project))
   (agg/entity :project {}
               (agg/->1 :customer :customer {:owned? false})
               (agg/->mn :members :person {:query-fn (agg/make-query-<many>-fn
                                                      :person
                                                      :person_project
                                                      :project_id
                                                      :person_id)
                                           :update-links-fn (agg/make-update-links-fn
                                                             :person_project
                                                             :project_id
                                                             :person_id)})
               (agg/->1 :manager :person {:owned? false})
               (agg/->n :tasks :task {:fk-kw :project_id}))
   (agg/entity :task {}
               (agg/->1 :project :project {:owned? false})
               (agg/->1 :assignee :person {:owned? false}))))
```

You can see that the library provides factories that create default DB
access functions based on core.java.jdbc. But you can use any other
functions that comply to the contract. Where you don't specify your own
function a default is provided.

The er-config above is complete, but it's dangerous because any
operation on a project might affect the whole database. So in order to
just take the necessary relationships into account we can narrow the
er-config down to support certain use cases.

```clojure
(def manage-person-to-project-er
  (-> er (agg/only [:person]
                   [:project :members :manager])))

(def manage-task-to-person-er
  "Supports task-to-person assignment, cutting off project related links."
  (-> er (agg/without [:task :project]
                      [:person :projects_as_member :projects_as_manager])))
```


This is all we need as preparation. Let's test it.
As a test scoped dependency H2 is available:

```clojure
(do (require '[aggregate.h2 :as h2])
    (h2/start-db))
; "Starting DB, web console is available on localhost:8082"
;= nil
```

Setup a schema

```clojure
(create-schema! @h2/db-con schema)
;= [0]
```

A save! could look like this:
```clojure
(def project (agg/save! er @h2/db-con :project
                                   {:name "Learning Clojure"
                                    :customer {:name "Big Company"}
                                    :tasks [{:desc "Buy a good book" :effort 1}
                                            {:desc "Install Java" :effort 2}
                                            {:desc "Configure Emacs" :effort 4}]
                                    :members [{:name "Daisy"}
                                              {:name "Mini"}]
                                    :manager {:name "Daisy"}}))
;= #'user/project									
(clojure.pprint/pprint project)
; {:aggregate.core/entity :project,
;  :id 2,
;  :name "Learning Clojure",
;  :manager_id 1,
;  :manager
;  {:name "Daisy", :tasks [], :aggregate.core/entity :person, :id 1},
;  :customer_id 1,
;  :customer
;  {:name "Big Company", :aggregate.core/entity :customer, :id 1},
;  :tasks
;  [{:desc "Buy a good book",
;    :effort 1,
;    :aggregate.core/entity :task,
;    :id 1}
;   {:desc "Install Java",
;    :effort 2,
;    :aggregate.core/entity :task,
;    :id 2}
;   {:desc "Configure Emacs",
;    :effort 4,
;    :aggregate.core/entity :task,
;    :id 3}],
;  :members
;  [{:name "Daisy", :tasks [], :aggregate.core/entity :person, :id 2}
;   {:name "Mini", :tasks [], :aggregate.core/entity :person, :id 3}]}
;= nil
```

We can easily inspect a table to check what has happened:
```clojure
(dump-table @h2/db-con :person_project [[:person_id 10] [:project_id 10]])
; person_id |project_id
; ----------|----------
; 2         |2
; 3         |2
;= nil
```

We can load a single task, without materializing it's project by using the narrowed er-config:
```clojure
(agg/load manage-task-to-person-er @h2/db-con :task 1)
;= {:aggregate.core/entity :task, :project_id 2, :effort 1, :desc "Buy a good book", :id 1}
```

And we can delete the project, and with it all it's owned entities and links to it:
```
(agg/delete! er @h2/db-con :project project)
;= 4
```


## License

Copyright © 2014 F.Riemenschneider

Distributed under the Eclipse Public License 1.0.
