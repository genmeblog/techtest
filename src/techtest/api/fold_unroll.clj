(ns techtest.api.fold-unroll
  (:require [tech.ml.dataset :as ds]

            [techtest.api.utils :refer [iterable-sequence?]]))

(defn unroll
  "Unroll columns if they are sequences. Selector can be one of:

  - column name
  - seq of column names
  - map: column name -> column type"
  [ds columns-selector]
  (cond
    (map? columns-selector) (reduce #(ds/unroll-column %1 (first %2) {:datatype (second %2)}) ds columns-selector)
    (iterable-sequence? columns-selector) (reduce #(ds/unroll-column %1 %2) ds columns-selector)
    :else (ds/unroll-column ds columns-selector)))

