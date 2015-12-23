(ns maxson.core
  (:require [maxson.gen :refer [dbs]]
            [clojure.java.jdbc :as jdbc]))

;; union (distinct)
;; sum
;; average
;; median
;; sort (order by)
;; min / max


;(defprotocol distributed
;  (reduce [this row] [this accum row])
;  (combine [x1 x2])
;  (finalize [x]))
;
;(defrecord Distributed [reduce combine finalize])
;
;(def average1
;  (->Distributed
;    #(vector (:total %) (:count %))
;    #(apply + %)
;    #(/ (% 0) (% 1))))
;
;(defrecord Average [column]
;  distributed
;  (reduce [_ row] [(column row) 1])
;  (reduce [_ [total count] row]
;    [(+ total (column row)) (inc count)])
;  (combine [[t1 c1] [t2 c2]]
;    [(+ t1 t2) (+ c1 c2)])
;  (finalize [[total count]]
;    (/ total count)))
;
;(def average2
;  (->Distributed
;    (fn [[total count] row]
;      [(+ total (:price row)) (inc count)])
;    #(apply + %)
;    #(/ (% 0) (% 1))))

(defn average
  [column]
  {:init     [0 0]
   :foldl    (fn [[total count] row]
               [(+ total (column row)) (inc count)])
   :combine  (fn [[t1 c1] [t2 c2]]
               [(+ t1 t2) (+ c1 c2)])
   :finalize (fn [[total count]]
               (/ total count))})

(defn process
  [dbs query {:keys [init foldl combine finalize]}]
  (loop [[d & ds] dbs
         rval init]
    (if d
      (let [r (reduce foldl init (jdbc/query d query))]
        (recur ds (combine rval r)))
      (finalize rval))))

(def q (partial jdbc/query {:classname "org.h2.Driver",
                            :subprotocol "h2:file",
                            :subname "./test/data/db00004",
                            :user "runner",
                            :password "runner"}))
(q ["select * from produce where name = ?" "lettuce"])

(def databases (maxson.gen/dbs 100))
(process
  databases
  ["select * from produce where name = ?" "lettuce"]
  (average :cost))