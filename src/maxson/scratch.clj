(ns maxson.scratch
  (:require [clojure.java.jdbc :as jdbc]))

(jdbc/db-do-commands db-spec
                     (jdbc/drop-table-ddl :fruit))
(jdbc/db-do-commands db-spec
                     (jdbc/create-table-ddl :fruit
                                            [:name "varchar(32)"]
                                            [:appearance "varchar(32)"]
                                            [:cost :int]
                                            [:grade :real]))

(jdbc/query db-spec "select * from fruit")

(def q (partial jdbc/query db-spec))

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


(def q (partial jdbc/query {:classname "org.h2.Driver",
                            :subprotocol "h2:file",
                            :subname "./test/data/db00004",
                            :user "runner",
                            :password "runner"}))
(q ["select * from produce where name = ?" "lettuce"])

(gen-all (dbs 100) 10000)