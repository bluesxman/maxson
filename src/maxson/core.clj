(ns maxson.core
  (:require [clojure.java.jdbc :as jdbc]))

(def db-spec
  {:classname   "org.h2.Driver"
   :subprotocol "h2:file"
   :subname     "./resources/db1"
   :user        "runner"
   :password    "runner"})

;(def conn (j/get-connection db-settings))

(jdbc/db-do-commands db-spec
                     (jdbc/create-table-ddl :fruit
                                            [:name "varchar(32)"]
                                            [:appearance "varchar(32)"]
                                            [:cost :int]
                                            [:grade :real]))

(jdbc/query db-spec "select * from fruit")

(jdbc/insert! db-spec :fruit
              {:name "banana"
               :appearance "green"
               :cost 1
               :grade 0.9}
              {:name "apple"
               :appearance "shiny"
               :cost 2
               :grade 0.95}
              {:name "tomato"
               :appearance "bruised"
               :cost 3
               :grade 0.3}
              )