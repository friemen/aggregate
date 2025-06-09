(defproject aggregate "1.1.4-SNAPSHOT"
  :description
  "Persisting complex datastructures in SQL tables"

  :url
  "https://github.com/friemen/aggregate"

  :license
  {:name "Eclipse Public License"
   :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[org.clojure/clojure "1.12.1"]
   [parsargs "1.2.0"]
   [org.clojure/java.jdbc "0.7.12"]]

  :plugins
  [[codox "0.8.10"]]

  :codox
  {:language     :clojure
   :source-paths ["src"]
   :output-path  "doc"
   :namespaces   [#"^aggregate"]
   :source-uri   "https://github.com/friemen/aggregate/blob/master/{filepath}#L{line}"}

  :profiles
  {:dev
   {:dependencies
    [[java-jdbc/dsl "0.1.3"]
     [com.h2database/h2 "2.3.232"]
     [org.postgresql/postgresql "9.3-1101-jdbc4"]]}}

  :scm
  {:name "git"
   :url "https://github.com/friemen/aggregate"}

  :repositories
  [["clojars" {:url "https://clojars.org/repo"
               :creds :gpg}]])
