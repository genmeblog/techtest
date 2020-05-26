(ns techtest.api.fold-unroll
  (:require [tech.ml.dataset :as ds]

            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.unique-by :refer [unique-by]]
            [techtest.api.columns :refer [column-names select-columns reorder-columns]]
            [techtest.api.group-by :refer [grouped? process-group-data]]))

(defn fold-by
  ([ds columns-selector] (fold-by ds columns-selector vec))
  ([ds columns-selector folding-function]
   (unique-by ds columns-selector {:strategy folding-function
                                   :add-group-as-column true})))

(defn- process-unroll
  [ds colnames-set colnames options]
  (let [unrolled-dss (map (fn [colname]
                            (let [opts (assoc options
                                              :datatype (get-in options [:datatypes colname] :object))]
                              [colname (-> ds
                                           (select-columns (complement (disj colnames-set colname)))
                                           (ds/unroll-column colname opts))])) colnames)]
    (-> (fn [[_ curr] [n uds]]
          [_ (ds/add-column curr (uds n))])
        (reduce unrolled-dss)
        (second)
        (reorder-columns (complement colnames-set)))))

(defn unroll
  ([ds columns-selector] (unroll ds columns-selector nil))
  ([ds columns-selector options]
   (let [colnames (column-names (if (grouped? ds)
                                  (first (ds :data))
                                  ds) columns-selector)
         colnames-set (set colnames)]
     (if (grouped? ds)
       (process-group-data ds #(process-unroll % colnames-set colnames options))
       (process-unroll ds colnames-set colnames options)))))
