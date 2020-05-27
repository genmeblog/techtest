(ns techtest.api.order-by
  (:require [tech.ml.dataset :as ds]
            
            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.group-by :refer [grouped? process-group-data]]))

(defn- comparator->fn
  [c]
  (cond
    (fn? c) c
    (= :desc c) (comp - compare)
    :else compare))

(defn asc-desc-comparator
  [orders]
  (if-not (iterable-sequence? orders)
    (comparator->fn orders)
    (if (every? #(= % :asc) orders)
      compare
      (let [comparators (map comparator->fn orders)]
        (fn [v1 v2]
          (loop [v1 v1
                 v2 v2
                 cmptrs comparators]
            (if-let [cmptr (first cmptrs)]
              (let [c (cmptr (first v1) (first v2))]
                (if-not (zero? c)
                  c
                  (recur (rest v1) (rest v2) (rest cmptrs))))
              0)))))))

(defn- sort-fn
  [columns-or-fn limit-columns comp-fn]
  (cond
    (iterable-sequence? columns-or-fn) (fn [ds]
                                         (ds/sort-by (apply juxt (map #(if (fn? %)
                                                                         %
                                                                         (fn [ds] (get ds %))) columns-or-fn))
                                                     comp-fn
                                                     limit-columns
                                                     ds))
    (fn? columns-or-fn) (fn [ds] (ds/sort-by columns-or-fn
                                            comp-fn
                                            limit-columns
                                            ds))
    :else (fn [ds] (ds/sort-by-column columns-or-fn comp-fn ds))))

(defn order-by
  "Order dataset by:
  - column name
  - columns (as sequence of names)
  - key-fn
  - sequence of columns / key-fn
  Additionally you can ask the order by:
  - :asc
  - :desc
  - custom comparator function"
  ([ds columns-or-fn] (order-by ds columns-or-fn nil))
  ([ds columns-or-fn comparators] (order-by ds columns-or-fn comparators nil))
  ([ds columns-or-fn comparators {:keys [limit-columns]}]
   (let [comparators (or comparators (if (iterable-sequence? columns-or-fn)
                                       (repeat (count columns-or-fn) :asc)
                                       [:asc]))
         sorting-fn (->> comparators
                         asc-desc-comparator
                         (sort-fn columns-or-fn limit-columns))]
     
     (if (grouped? ds)
       (process-group-data ds sorting-fn)
       (sorting-fn ds)))))


