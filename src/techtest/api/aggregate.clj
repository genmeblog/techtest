(ns techtest.api.aggregate
  (:require [techtest.api.utils :refer [iterable-sequence? ->str]]
            [techtest.api.group-by :refer [grouped? process-group-data ungroup]]
            [techtest.api.dataset :refer [dataset]]))

(defn- add-agg-result
  [tot-res k agg-res]
  (cond
    (map? agg-res) (reduce conj tot-res agg-res)
    (iterable-sequence? agg-res) (->> agg-res
                                      (map-indexed vector)
                                      (reduce (fn [b [id v]]
                                                (conj b [(keyword (str (->str k) "-" id)) v])) tot-res))
    :else (conj tot-res [k agg-res])))

(defn- aggregate-map
  [ds aggregator options]
  (dataset (reduce (fn [res [k f]]
                     (add-agg-result res k (f ds))) [] aggregator) options))

(defn aggregate
  "Aggregate dataset by providing:

  - aggregation function
  - map with column names and functions
  - sequence of aggregation functions

  Aggregation functions can return:
  - single value
  - seq of values
  - map of values with column names"
  ([ds aggregator] (aggregate ds aggregator nil))
  ([ds aggregator {:keys [default-column-name-prefix ungroup?]
                   :or {default-column-name-prefix "summary" ungroup? true}
                   :as options}]

   (let [aggregator (cond
                      (fn? aggregator) {:summary aggregator}
                      (iterable-sequence? aggregator) (->> aggregator
                                                           (interleave (map #(->> %
                                                                                  (str default-column-name-prefix "-")
                                                                                  keyword) (range)))
                                                           (apply array-map))
                      :else aggregator)]
     
     (if (grouped? ds)
       (let [pds (process-group-data ds #(aggregate-map % aggregator options))]
         (if ungroup?
           (ungroup pds (merge {:add-group-as-column true} options))
           pds))
       (aggregate-map ds aggregator options)))))
