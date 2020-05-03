(ns techtest.api
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
            [tech.ml.protocols.dataset :as prot]))

;; attempt to reorganized api

;;;;;;;;;;;;;;;;;;;;;
;; DATASET CREATION
;;;;;;;;;;;;;;;;;;;;;

(defn- map-v [f coll]
  (reduce-kv (fn [m k v] (assoc m k (f v))) (empty coll) coll))

(defn- map-kv [f coll]
  (reduce-kv (fn [m k v] (assoc m k (f k v))) (empty coll) coll))

(defn dataset?
  [ds]
  (satisfies? prot/PColumnarDataset ds))

(defn sequential+?
  [xs]
  (and (not (map? xs))
       (or (sequential? xs)
           (col/is-column? xs)
           (instance? Iterable xs))))

(defn- fix-map-dataset
  "If map contains value which is not a sequence, convert to a sequence."
  [map-ds]
  (let [c (if-let [first-seq (first (filter sequential+? (vals map-ds)))]
            (count first-seq)
            1)]
    (map-v #(if (sequential+? %) % (repeat c %)) map-ds)))

(defn dataset
  "Dataset can be created from:

  * single value
  * map of values or sequences
  * sequence of maps
  * file"
  ([ds]
   (dataset ds nil))
  ([ds options]
   (cond
     (dataset? ds) ds
     (map? ds) (apply ds/name-values-seq->dataset (fix-map-dataset ds) options)
     (and (sequential+? ds)
          (every? sequential+? ds)
          (every? #(= 2 (count %)) ds)) (dataset (into {} ds) options)
     (not (seqable? ds)) (ds/->dataset [{0 ds}] options)
     :else (ds/->dataset ds options))))

(dataset 999)
(dataset [[:A 33]])
(dataset {:A 33})
(dataset {:A [1 2 3]})
(dataset {:A [3 4 5] :B "X"})
(dataset [[:A [3 4 5]] [:B "X"]])
(dataset [{:a 1 :b 3} {:b 2 :a 99}])
;; https://archive.ics.uci.edu/ml/machine-learning-databases/kddcup98-mld/epsilon_mirror/
;; (time (def ds (dataset "cup98LRN.txt.gz")))
(def DS (dataset {:V1 (take 9 (cycle [1 2]))
                  :V2 (range 1 10)
                  :V3 (take 9 (cycle [0.5 1.0 1.5]))
                  :V4 (take 9 (cycle [\A \B \C]))}))

;;;;;;;;;;;;;
;; GROUPING
;;;;;;;;;;;;;

(defn grouped? [ds] (:grouped? (meta ds)))

(defn unmark-group
  "Remove grouping tag"
  [ds]
  (vary-meta ds dissoc :grouped?))

(defn mark-as-group
  "Add grouping tag"
  [ds]
  (vary-meta ds assoc :grouped? true))

;; TODO: maybe make possible nested grouping and ungrouping?
(defn group-ds-by
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
  - data - group as dataset
  - indexes - group as seq of row indexes"
  ([ds key-fn-or-columns-or-map] (group-ds-by ds key-fn-or-columns-or-map nil))
  ([ds key-fn-or-columns-or-map {:keys [limit-columns result-type]
                                 :or {result-type :as-dataset}
                                 :as options}]
   (let [group-indexes (cond
                         (map? key-fn-or-columns-or-map) key-fn-or-columns-or-map 
                         (sequential+? key-fn-or-columns-or-map) (ds/group-by->indexes identity key-fn-or-columns-or-map ds)
                         (fn? key-fn-or-columns-or-map) (ds/group-by->indexes key-fn-or-columns-or-map limit-columns ds)
                         :else (ds/group-by-column->indexes key-fn-or-columns-or-map ds))]
     (condp = result-type
       :as-indexes group-indexes
       :as-map (into {} (map (fn [[k v]] [k (ds/select ds :all v)]) group-indexes))
       (-> (->> group-indexes
                (map-indexed (fn [id [k idxs]]
                               {:name k
                                :group-id id
                                :count (count idxs)
                                :data (ds/select ds :all idxs)
                                :indexes (vec idxs)})))
           (dataset options)
           (mark-as-group))))))

(group-ds-by DS :V1 {:result-type :as-map})
(group-ds-by DS [:V1 :V3])
(group-ds-by DS (juxt :V1 :V4))
(group-ds-by DS #(< (:V2 %) 4))
(group-ds-by DS (comp #{\B} :V4))

(defn ungroup
  "Concat groups into dataset"
  ([ds] (ungroup ds nil))
  ([ds {:keys [order add-group-as-column add-group-id-as-column dataset-name]}]
   (assert (grouped? ds) "Work only on grouped dataset")
   (let [prepared-ds (->> ds
                          (ds/mapseq-reader)
                          (map (fn [{:keys [name group-id count data] :as ds}]
                                 (let [data (if add-group-as-column
                                              (ds/add-or-update-column data :_group-name_ (repeat count name))
                                              (if add-group-id-as-column
                                                (ds/add-or-update-column data :_group-id_ (repeat count group-id))
                                                data))]
                                   (assoc ds :data data)))))
         ordered-ds (condp = order
                      :by-group (sort-by :name prepared-ds)
                      :by-group-id (sort-by :group-id prepared-ds)
                      prepared-ds)]
     (-> (apply ds/concat (map :data ordered-ds))
         (ds/set-dataset-name dataset-name)))))


(ungroup (group-ds-by DS :V1) {:dataset-name "ungrouped"})
(ungroup (group-ds-by DS [:V1 :V3]) {:add-group-id-as-column true})
(ungroup (group-ds-by DS (juxt :V1 :V4)) {:add-group-as-column true})
(ungroup (group-ds-by DS #(< (:V2 %) 4)) {:add-group-as-column true})
(ungroup (group-ds-by DS (comp #{\B \C} :V4)) {:add-group-as-column true})

(defn group-as-map
  [ds]
  (assert (grouped? ds) "Apply on grouped dataset only")
  (zipmap (ds :name) (ds :data)))

(group-as-map (group-ds-by DS (juxt :V1 :V4)))

;;;;;;;;;;;;;;;;;;;;;;;
;; COLUMNS OPERATIONS
;;;;;;;;;;;;;;;;;;;;;;;

(defn- select-or-drop-columns
  "Select columns by (returns dataset):

  - name
  - sequence of names
  - map of names with new names (rename)
  - function which filter names (via column metadata)"
  [f ds cols-selector]
  (if (grouped? ds)
    (ds/add-or-update-column ds :data (map #(select-or-drop-columns f % cols-selector) (ds :data)))
    (f ds (cond
            (or (map? cols-selector)
                (sequential+? cols-selector)) cols-selector
            (fn? cols-selector) (map :name (filter cols-selector (map meta (ds/columns ds))))
            :else [cols-selector]))))

(def select-columns (partial select-or-drop-columns ds/select-columns))
(def drop-columns (partial select-or-drop-columns ds/drop-columns))
(defn rename-columns
  [ds col-map]
  (if (grouped? ds)
    (ds/add-or-update-column ds :data (map #(ds/rename-columns % col-map) (ds :data)))
    (ds/rename-columns ds col-map)))

(select-columns DS :V1)
(select-columns DS [:V1 :V2])
(select-columns DS {:V1 "v1"
                    :V2 "v2"})
(select-columns DS (comp #{:int64} :datatype))

(drop-columns DS :V1)
(drop-columns DS [:V1 :V2])
(drop-columns DS (comp #{:int64} :datatype))

(rename-columns DS {:V1 "v1"
                    :V2 "v2"})

(ungroup (select-columns (group-ds-by DS :V1) {:V1 "v1"}))
(ungroup (drop-columns (group-ds-by DS [:V4]) (comp #{:int64} :datatype)))

(defn- fix-column-size
  [col-or-seq strategy cnt]
  (let [seq-cnt (count col-or-seq)]
    (cond
      (> seq-cnt cnt) (take cnt col-or-seq)
      (< seq-cnt cnt) (if (= strategy :cycle)
                        (take cnt (cycle col-or-seq))
                        (concat col-or-seq (repeat (- cnt seq-cnt) nil)))
      :else col-or-seq)))

(defn add-or-update-column
  ([ds col-name col-seq-or-fn] (add-or-update-column ds col-name col-seq-or-fn nil))
  ([ds col-name col-seq-or-fn {:keys [count-strategy]
                               :or {count-strategy :cycle}
                               :as options}]
   (if (grouped? ds)
     (ds/add-or-update-column ds :data (map #(add-or-update-column % col-name col-seq-or-fn options) (ds :data)))
     (cond
       (or (col/is-column? col-seq-or-fn)
           (sequential+? col-seq-or-fn)) (ds/add-or-update-column ds col-name (fix-column-size col-seq-or-fn
                                                                                               count-strategy
                                                                                               (ds/row-count ds)))
       (fn? col-seq-or-fn) (add-or-update-column ds col-name (col-seq-or-fn ds))
       :else (ds/add-or-update-column ds col-name (repeat (ds/row-count ds) col-seq-or-fn))))))

(defn add-or-update-columns
  ([ds map-of-cols] (add-or-update-columns ds map-of-cols nil))
  ([ds map-of-cols options]
   (reduce (fn [ds [k v]]
             (add-or-update-column ds k v options)) ds map-of-cols)))

(add-or-update-column DS :abc [1 2] {:count-strategy :na})
(add-or-update-column DS :abc [1 2] {:count-strategy :cycle})
(add-or-update-column DS :abc "X")
(add-or-update-columns DS {:abc "X"
                           :xyz [1 2 3]})
(add-or-update-column DS :abc (fn [ds] (dfn/+ (ds :V1) (ds :V2))))

(add-or-update-columns DS {:abc "X"
                           :xyz [1 2 3]} {:count-strategy :na})

(ungroup (add-or-update-column (group-ds-by DS :V4) :abc #(let [mean (dfn/mean (% :V2))
                                                                stddev (dfn/standard-deviation (% :V2))]
                                                            (dfn// (dfn/- (% :V2) mean)
                                                                   stddev))))


(ungroup (add-or-update-columns (group-ds-by DS :V4) {:abc "X"
                                                      :xyz [-1]} {:count-strategy :na}))

;;;;;;;;;;;;;;;;;;;;
;; ROWS OPERATIONS
;;;;;;;;;;;;;;;;;;;;

(defn- find-indices
  ([ds rows-selector] (find-indices ds rows-selector nil))
  ([ds rows-selector limit-columns]
   (cond
     (number? rows-selector) [(long rows-selector)]
     (sequential+? rows-selector) (if (number? (first rows-selector))
                                    rows-selector
                                    (->> rows-selector
                                         (take (ds/row-count ds))
                                         (map-indexed vector)
                                         (filter second)
                                         (map first)))
     (fn? rows-selector) (->> (or limit-columns :all)
                              (ds/select-columns ds)
                              (ds/mapseq-reader)
                              (map-indexed vector)
                              (filter (comp rows-selector second))
                              (map first)))))

(find-indices DS 1)
(find-indices DS [2 3])
(find-indices DS [true nil nil true])
(find-indices DS (comp #(< 1 %) :V1))

(defn- select-or-drop-rows
  "Select or drop rows using:

  - row id
  - seq of row ids
  - seq of true/false
  - fn with predicate"
  ([f ds rows-selector] (select-or-drop-rows f ds rows-selector nil))
  ([f ds rows-selector {:keys [limit-columns pre]
                        :or {pre identity}
                        :as options}]
   (if (grouped? ds)
     (let [pre-ds (map pre (ds :data))
           indices (map #(find-indices % rows-selector limit-columns) pre-ds)]
       (-> ds
           (ds/add-or-update-column :data (map #(select-or-drop-rows f %1 %2) (ds :data) indices))
           (ds/add-or-update-column :indexes (map #(mapv %1 %2) (ds :indexes) indices))
           (ds/add-or-update-column :count (map ds/row-count (ds :data)))))
     (f ds (find-indices (pre ds) rows-selector limit-columns)))))

(def select-rows (partial select-or-drop-rows ds/select-rows))
(def drop-rows (partial select-or-drop-rows ds/drop-rows))

(select-rows DS 3)
(drop-rows DS 3)

(select-rows DS [3 4 7])
(drop-rows DS [3 4 7])

(select-rows DS (comp #(< 1 %) :V1))
(drop-rows DS (comp #(< 1 %) :V1))

(select-rows DS [true nil nil true])
(drop-rows DS [true nil nil true])

(ungroup (select-rows (group-ds-by DS :V1) 0 {:pre #(add-or-update-column % :mean (dfn/mean (% :V2)))}))
(ungroup (drop-rows (group-ds-by DS :V1) 0))

(select-rows DS (fn [row] (<= (:V2 row) (:mean row))) {:pre #(add-or-update-column % :mean (dfn/mean (% :V2)))})
(ungroup (select-rows (group-ds-by DS :V4)
                      (fn [row] (< (:V2 row) (:mean row)))
                      {:pre #(add-or-update-column % :mean (dfn/mean (% :V2)))}))


;;;;;;;;;;;;;;;;
;; AGGREGATING
;;;;;;;;;;;;;;;;

(defn aggregate
  [ds fn-map-or-seq]
  (cond
    (fn? fn-map-or-seq) (aggregate ds {:summary fn-map-or-seq})
    (sequential+? fn-map-or-seq) (zipmap (map #(keyword (str "summary-" %)) (range)) fn-map-or-seq)
    :else (dataset (map-v #(% ds) fn-map-or-seq))))


;;;;;;;;;;;;;
;; ORDERING
;;;;;;;;;;;;;
