(ns aggregate.h2
  "Manage in process DB H2 for testing purposes."
  (:require [clojure.java.jdbc :as jdbc])
  (:import [org.h2.tools Server]))

(defonce db-server (atom nil))

(defn start-db
  "Starts H2 server including web console (available on localhost:8082)."
  []
  (when-not @db-server
    (println "Starting DB, web console is available on localhost:8082")
    (reset! db-server {:tcp (Server/createTcpServer (into-array String []))
                       :web (Server/createWebServer (into-array String []))})
    (doseq [s (vals @db-server)] (.start s))))


(defn stop-db
  "Stops H2 server including web console."
  []
  (when-let [s @db-server]
    (println "Stopping DB")
    (doseq [s (vals s)] (.stop s))
    (reset! db-server nil)))

    
(def db-spec {:classname "org.h2.Driver"
              :subprotocol "h2"
              :subname "tcp://localhost/~/test"
              :user "sa"
              :password ""})

(def db-con (delay 
             {:connection (jdbc/get-connection db-spec)}))              
