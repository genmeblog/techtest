(ns techtest.api.group-by
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype :as dtype]
            
            [techtest.api.utils :refer [iterable-sequence? ->str column-names]]
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

(defn- mark-as-group
  "Add grouping tag"
  [ds]
  (vary-meta ds assoc
             :grouped? true
             :print-line-policy :single))

(def as-regular-dataset unmark-group)

(defn- find-group-indexes
  "Calulate indexes for groups"
  [ds grouping-selector selected-keys]
  (cond
    (map? grouping-selector) grouping-selector
    (iterable-sequence? grouping-selector) (ds/group-by->indexes identity grouping-selector ds)
    (fn? grouping-selector) (ds/group-by->indexes grouping-selector selected-keys ds)
    :else (ds/group-by-column->indexes grouping-selector ds)))

(defn- subdataset
  [ds id k idxs]
  (-> ds
      (ds/select :all idxs)
      (ds/set-dataset-name (str "Group: " k))
      (vary-meta assoc :group-id id)))

(defn- group-indexes->map
  "Create map representing grouped dataset from indexes"
  [ds id [k idxs]]
  {:name k
   :group-id id
   :data (subdataset ds id k idxs)})

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

  - select-keys - when grouping is done by function, you can limit fields to a `select-keys` seq.
  - result-type - return results as dataset (`:as-dataset`, default) or as map of datasets (`:as-map`) or as map of row indexes (`:as-indexes`) or as sequence of (sub)datasets
  - other parameters which are passed to `dataset` fn

  When dataset is returned, meta contains `:grouped?` set to true. Columns in dataset:

  - name - group name
  - group-id - id of the group (int)
  - data - group as dataset"
  ([ds grouping-selector] (group-by ds grouping-selector nil))
  ([ds grouping-selector {:keys [select-keys result-type]
                          :or {result-type :as-dataset}
                          :as options}]
   (let [selected-keys (when select-keys (column-names ds select-keys))
         group-indexes (find-group-indexes ds grouping-selector selected-keys)]
     (condp = result-type
       :as-indexes group-indexes
       :as-seq (->> group-indexes ;; java.util.HashMap
                    (map-indexed (fn [id [k idxs]] (subdataset ds id k idxs))))
       :as-map (->> group-indexes ;; java.util.HashMap
                    (map-indexed (fn [id [k idxs]] [k (subdataset ds id k idxs)])) 
                    (into {}))
       (group-by->dataset ds group-indexes options)))))

(defn process-group-data  
  [ds f]
  (ds/add-or-update-column ds :data (map f (ds :data))))

(defn groups->map
  "Convert grouped dataset to the map of groups"
  [ds]
  (assert (grouped? ds) "Apply on grouped dataset only")
  (zipmap (ds :name) (ds :data)))

(defn groups->seq
  [ds]
  (assert (grouped? ds) "Apply on grouped dataset only")
  (seq (ds :data)))

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

(defn- maybe-name
  [possible-name default-name]
  (if (true? possible-name)
    default-name
    possible-name))

(defn- group-name-seq->cols
  [name add-group-as-column count]
  (let [cn (if (iterable-sequence? add-group-as-column)
             add-group-as-column
             (let [tn (maybe-name add-group-as-column :$group-name)]
               (map-indexed #(keyword (str %2 "-" %1)) (repeat (->str tn)))))]
    (group-name-map->cols (map vector cn name) count)))

(defn- group-as-column->seq
  "Convert group name to a seq of columns"
  [add-group-as-column separate? name count]
  (when add-group-as-column
    (cond
      (and separate? (map? name)) (group-name-map->cols name count)
      (and separate? (iterable-sequence? name)) (group-name-seq->cols name add-group-as-column count)
      :else [(col/new-column (maybe-name add-group-as-column :$group-name) (repeat count name))])))

(defn- group-id-as-column->seq
  "Convert group id to as seq of columns"
  [add-group-id-as-column count group-id]
  (when add-group-id-as-column
    [(col/new-column (maybe-name add-group-id-as-column :$group-id) (dtype/const-reader group-id count {:datatype :int64}))]))

(defn- prepare-ds-for-ungrouping
  "Add optional group name and/or group-id as columns to a result of ungrouping."
  [ds add-group-as-column add-group-id-as-column separate?]
  (->> ds
       (ds/mapseq-reader)
       (map (fn [{:keys [name group-id data] :as ds}]
              (if (or add-group-as-column
                      add-group-id-as-column)
                (let [count (ds/row-count data)]
                  (add-groups-as-columns ds
                                         (group-as-column->seq add-group-as-column separate? name count)
                                         (group-id-as-column->seq add-group-id-as-column count group-id)
                                         (ds/columns data)))
                ds)))))

(defn- order-ds-for-ungrouping
  "Order results by group name or leave untouched."
  [prepared-ds order-by-group?]
  (cond
    (= :desc order-by-group?) (sort-by :name #(compare %2 %1) prepared-ds)
    order-by-group? (sort-by :name prepared-ds)
    :else prepared-ds))

(defn ungroup
  "Concat groups into dataset.

  When `add-group-as-column` or `add-group-id-as-column` is set to `true` or name(s), columns with group name(s) or group id is added to the result.

  Before joining the groups groups can be sorted by group name."
  ([ds] (ungroup ds nil))
  ([ds {:keys [order? add-group-as-column add-group-id-as-column separate? dataset-name]
        :or {separate? true}}]
   (assert (grouped? ds) "Works only on grouped dataset")
   (-> ds
       (prepare-ds-for-ungrouping add-group-as-column add-group-id-as-column separate?)
       (order-ds-for-ungrouping order?)
       (->> (map :data)
            (apply ds/concat))
       (ds/set-dataset-name (or dataset-name (ds/dataset-name ds))))))
