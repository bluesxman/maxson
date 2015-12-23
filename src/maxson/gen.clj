(ns maxson.gen
  (:require [clojure.java.jdbc :as jdbc]))

(def db-dir "./test/data")

(def db-spec
  {:classname   "org.h2.Driver"
   :subprotocol "h2:file"
   :subname     "./test/data/db1"
   :user        "runner"
   :password    "runner"})

(def fruit #{"apple" "banana" "strawberries" "peach" "pear"})
(def vegetable #{"carrot" "lettuce" "peas" "celery"})
(def item-type (into [] (concat fruit vegetable)))
(def appearance ["green" "shiny" "bruised"])


(defn table?
  [conn table-name]
  (let [name (.toUpperCase
               (if (keyword? table-name)
                 (name table-name)
                 table-name))]
    (not-empty (jdbc/query
       conn
       ["select * from information_schema.tables where table_name = ?"
        name]))))

(defn init
  [db-spec]
  (jdbc/get-connection db-spec)
  (when
    (table? db-spec :produce)
    (jdbc/db-do-commands db-spec
                        (jdbc/drop-table-ddl :produce)))
  (jdbc/db-do-commands db-spec
                       (jdbc/create-table-ddl :produce
                                              [:name "varchar(32)"]
                                              [:appearance "varchar(32)"]
                                              [:cost :real]
                                              [:quantity :int]
                                              [:grade :real])))

(defn item
  []
  {:name (rand-nth item-type)
   :appearance (rand-nth appearance)
   :cost (rand 10.0)
   :quantity (rand-int 1000)
   :grade (rand)})

(defn populate
  [table generator num-rows db-spec]
  (apply jdbc/insert!
    db-spec
    table
    (repeatedly num-rows generator)))

(def q (partial jdbc/query db-spec))

(defn spec
  [db-spec db-dir i]
  (assoc db-spec :subname (format "%s/db%05d" db-dir i)))

(defn dbs
  [base-spec db-dir num-dbs]
  (map (partial spec base-spec db-dir) (range num-dbs)))

(defn gen-all
  [spec dir db-count rows]
  (doseq [db (dbs spec dir db-count)]
    (init db)
    (populate :produce item rows db)))