(ns maxson.core
  (:require [maxson.gen :refer [dbs]]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.reducers :as r]))

;; union (distinct)
;; sum
;; average
;; median
;; sort (order by)
;; min / max
;; filter/remove


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
  {:fold     (fn
               ([] [0 0])
               ([[total count] row]
                [(+ total (column row)) (inc count)]))
   :combine  (fn
               ([] [0 0])
               ([[t1 c1] [t2 c2]]
                  [(+ t1 t2) (+ c1 c2)]))
   :finalize (fn [[total count]]
               (/ total count))})

(defn process
  [dbs query {:keys [fold combine finalize]}]
  (loop [[d & ds] dbs
         rval (fold)]
    (if d
      (let [r (reduce fold (fold) (jdbc/query d query))]
        (recur ds (combine rval r)))
      (finalize rval))))

(def group-size 512)
(defn process
  [dbs query {:keys [fold combine finalize]}]
  (let [db-fold (fn
                  ([] (fold))
                  ([accum db]
                   (combine
                     accum
                     (reduce fold (fold) (jdbc/query db query)))))]
    (finalize (r/fold group-size combine db-fold dbs))))


(def databases (dbs 100))
(process
  databases
  ["select * from produce where name = ?" "lettuce"]
  (average :cost))