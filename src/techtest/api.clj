(ns techtest.api
  (:refer-clojure :exclude [group-by drop concat rand-nth first last shuffle])
  (:require [tech.parallel.utils :as exporter]))

(exporter/export-symbols tech.v2.datatype
                         clone)

(exporter/export-symbols tech.ml.dataset
                         column-count
                         row-count
                         set-dataset-name
                         dataset-name
                         column
                         has-column?
                         write-csv!
                         dataset->str
                         concat)

(exporter/export-symbols techtest.api.dataset
                         dataset?
                         dataset
                         shape
                         info
                         columns
                         rows
                         print-dataset)

(exporter/export-symbols techtest.api.group-by
                         group-by
                         ungroup
                         grouped?
                         unmark-group
                         as-regular-dataset
                         process-group-data
                         groups->seq
                         groups->map)

(exporter/export-symbols techtest.api.columns
                         column-names
                         select-columns
                         drop-columns
                         rename-columns
                         add-or-update-column
                         add-or-update-columns
                         map-columns
                         reorder-columns
                         convert-column-type
                         ->array)

(exporter/export-symbols techtest.api.rows
                         select-rows
                         drop-rows
                         head
                         tail
                         shuffle
                         random
                         rand-nth
                         first
                         last
                         by-rank)

(exporter/export-symbols techtest.api.aggregate
                         aggregate
                         aggregate-columns)

(exporter/export-symbols techtest.api.order-by
                         order-by)

(exporter/export-symbols techtest.api.unique-by
                         unique-by)

(exporter/export-symbols techtest.api.missing
                         select-missing
                         drop-missing
                         replace-missing)

(exporter/export-symbols techtest.api.join-separate
                         join-columns
                         separate-column)

(exporter/export-symbols techtest.api.fold-unroll
                         fold-by
                         unroll)

(exporter/export-symbols techtest.api.reshape
                         pivot->longer
                         pivot->wider)

(exporter/export-symbols techtest.api.join-concat-ds                         
                         left-join
                         right-join
                         inner-join
                         full-join
                         semi-join
                         anti-join
                         intersect
                         difference
                         union)

;;

(defn- select-or-drop
  "Select columns and rows"
  [fc fs ds columns-selector rows-selector]
  (let [ds (if (and columns-selector
                    (not= :all columns-selector))
             (fc ds columns-selector)
             ds)]
    (if (and rows-selector
             (not= :all rows-selector))
      (fs ds rows-selector)
      ds)))

(def select (partial select-or-drop select-columns select-rows))
(def drop (partial select-or-drop drop-columns drop-rows))

