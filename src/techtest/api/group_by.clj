(ns techtest.api.group-by
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype :as dtype]
            

            [techtest.api.utils :refer [map-v iterable-sequence?]]
            [techtest.api.dataset :refer [dataset]])
  (:refer-clojure :exclude [group-by]))

(defn grouped?
  "Is `dataset` represents grouped dataset (result of `group-by`)?"
  [ds]
  (:grouped? (meta ds)))

(defn unmark-group
  "Remove grouping tag"
  [ds]
  (vary-meta ds dissoc :grouped?))

(defn mark-as-group
  "Add grouping tag"
  [ds]
  (vary-meta ds assoc :grouped? true))

(def as-regular-dataset unmark-group)

(defn- find-group-indexes
  "Calulate indexes for groups"
  [grouping-selector ds limit-columns]
  (cond
    (map? grouping-selector) grouping-selector
    (iterable-sequence? grouping-selector) (ds/group-by->indexes identity grouping-selector ds)
    (fn? grouping-selector) (ds/group-by->indexes grouping-selector limit-columns ds)
    :else (ds/group-by-column->indexes grouping-selector ds)))

(defn- group-indexes->map
  "Create map representing grouped dataset from indexes"
  [ds id [k idxs]]
  (let [cnt (count idxs)]
    {:name k
     :group-id id
     :count cnt
     :data (vary-meta (ds/set-dataset-name (ds/select ds :all idxs) k) assoc :group-id id)}))

(defn- group-by->dataset
  "Create grouped dataset from indexes"
  [ds group-indexes options]
  (-> (map-indexed (partial group-indexes->map ds) group-indexes)
      (dataset options)
      (mark-as-group)))

;; TODO: maybe make possible nested grouping and ungrouping?
(defn group-by
  "Group dataset by:

  - column name
  - list of columns
  - map of keys and row indexes
  - function getting map of values

  Options are:

  - limit-columns - when grouping is done by function, you can limit fields to a `limit-columns` seq.
  - result-type - return results as dataset (`:as-dataset`, default) or as map of datasets (`:as-map`) or as map of row indexes (`:as-indexes`)
  - other parameters which are passed to `dataset` fn

  When dataset is returned, meta contains `:grouped?` set to true. Columns in dataset:

  - name - group name
  - group-id - id of the group (int)
  - count - number of rows in group
  - data - group as dataset"
  ([ds grouping-selector] (group-by ds grouping-selector nil))
  ([ds grouping-selector {:keys [limit-columns result-type]
                          :or {result-type :as-dataset}
                          :as options}]
   (let [group-indexes (find-group-indexes grouping-selector ds limit-columns)]
     (condp = result-type
       :as-indexes group-indexes
       :as-map (->> group-indexes ;; java.util.HashMap
                    (map (fn [[k v]] [k (ds/select ds :all v)])) 
                    (into {}))
       (group-by->dataset ds group-indexes options)))))

(defn correct-group-count
  "Correct count for each group"
  [ds]
  (ds/column-map ds :count ds/row-count :data))

(defn process-group-data
  ([ds f] (process-group-data ds f false))
  ([ds f corr-count?]
   (let [temp (ds/column-map ds :data f :data)]
     (if corr-count?
       (correct-group-count temp)
       temp))))

(defn group-as-map
  "Convert grouped dataset to the map of groups"
  [ds]
  (assert (grouped? ds) "Apply on grouped dataset only")
  (zipmap (ds :name) (ds :data)))

;; ungrouping

(defn- add-groups-as-columns
  "Add group columns to the result of ungrouping"
  [ds col1 col2 columns]
  (->> columns
       (clojure.core/concat col1 col2)
       (remove nil?)
       (ds/new-dataset)
       (assoc ds :data)))

(defn- group-name-map->cols
  "create columns with repeated value `v` from group name if it's map."
  [name count]
  (map (fn [[n v]]
         (col/new-column n (repeat count v))) name))

(defn- group-as-column->seq
  "Convert group name to a seq of columns"
  [add-group-as-column? name count]
  (if add-group-as-column?
    (if (map? name)
      (group-name-map->cols name count)
      [(col/new-column :$group-name (repeat count name))])))

(defn- group-id-as-column->seq
  "Convert group id to as seq of columns"
  [add-group-id-as-column? count group-id]
  (if add-group-id-as-column?
    [(col/new-column :$group-id (dtype/const-reader group-id count {:datatype :int64}))]))

(defn- prepare-ds-for-ungrouping
  "Add optional group name and/or group-id as columns to a result of ungrouping."
  [ds add-group-as-column? add-group-id-as-column?]
  (->> ds
       (ds/mapseq-reader)
       (map (fn [{:keys [name group-id count data] :as ds}]
              (if (or add-group-as-column?
                      add-group-id-as-column?)
                (add-groups-as-columns ds
                                       (group-as-column->seq add-group-as-column? name count)
                                       (group-id-as-column->seq add-group-id-as-column? count group-id)
                                       (ds/columns data))
                ds)))))

(defn- order-ds-for-ungrouping
  "Order results by group name or leave untouched."
  [prepared-ds order-by-group?]
  (if order-by-group?
    (sort-by :name prepared-ds)
    prepared-ds))

(defn ungroup
  "Concat groups into dataset.

  When `add-group-as-column?` or `add-group-id-as-column?` is set to `true`, columns with group name(s) or group id is added to thre result.

  Before joining the groups groups can be sorted by group name. "
  ([ds] (ungroup ds nil))
  ([ds {:keys [order-by-group? add-group-as-column? add-group-id-as-column? dataset-name]}]
   (assert (grouped? ds) "Work only on grouped dataset")
   (-> ds
       (prepare-ds-for-ungrouping add-group-as-column? add-group-id-as-column?)
       (order-ds-for-ungrouping order-by-group?)
       (->> (map :data)
            (apply ds/concat))
       (ds/set-dataset-name dataset-name))))
