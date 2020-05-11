(ns techtest.api
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.v2.datatype.readers.concat :as concat-rdr]
            [tech.ml.dataset.pipeline :as pipe]
            [tech.ml.protocols.dataset :as prot]
            [clojure.string :as str]
            [clojure.set :as set])
  (:refer-clojure :exclude [group-by drop]))

;; attempt to reorganized api

;;;;;;;;;;;;
;; HELPERS
;;;;;;;;;;;;

(defn- map-v [f coll] (reduce-kv (fn [m k v] (assoc m k (f v))) (empty coll) coll))
(defn- map-kv [f coll] (reduce-kv (fn [m k v] (assoc m k (f k v))) (empty coll) coll))

;;;;;;;;;;;;;;;;;;;;;
;; DATASET CREATION
;;;;;;;;;;;;;;;;;;;;;

(defn dataset?
  "Is `ds` a `dataset` type?"
  [ds]
  (satisfies? prot/PColumnarDataset ds))

(defn- sequential+?
  "Check if object is sequencial, is column or maybe a reader?"
  [xs]
  (and (not (map? xs))
       (or (sequential? xs)
           (col/is-column? xs)
           (instance? Iterable xs))))

(defn- fix-map-dataset
  "If map contains value which is not a sequence, convert ir to a sequence."
  [map-ds]
  (let [c (if-let [first-seq (->> map-ds
                                  (vals)
                                  (filter sequential+?)
                                  (first))]
            (count first-seq)
            1)]
    (map-v #(if (sequential+? %) % (repeat c %)) map-ds)))

(defn unroll
  "Unroll columns if they are sequences. Selector can be one of:

  - column name
  - seq of column names
  - map: column name -> column type"
  [ds columns-selector]
  (cond
    (map? columns-selector) (reduce #(ds/unroll-column %1 (first %2) {:datatype (second %2)}) ds columns-selector)
    (sequential+? columns-selector) (reduce #(ds/unroll-column %1 %2) ds columns-selector)
    :else (ds/unroll-column ds columns-selector)))

(defn dataset
  "Create `dataset`.
  
  Dataset can be created from:

  * single value
  * map of values and/or sequences
  * sequence of maps
  * sequence of columns
  * file or url

  If `unroll` is set, `unroll` function is called."
  ([data]
   (dataset data nil))
  ([data {:keys [single-value-name]
          :or {single-value-name :$value}
          :as options}]
   (let [ds (cond
              (dataset? data) data
              (map? data) (apply ds/name-values-seq->dataset (fix-map-dataset data) options)
              (and (sequential+? data)
                   (every? sequential+? data)
                   (every? #(= 2 (count %)) data)) (dataset (into {} data) options)
              (and (sequential+? data)
                   (every? col/is-column? data)) (ds/new-dataset options data)
              (not (seqable? data)) (ds/->dataset [{single-value-name data}] options)
              :else (ds/->dataset data options))]
     (if-let [unroll-selector (:unroll options)]
       (unroll ds unroll-selector)
       ds))))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DESCRIPTIVE FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn columns-info
  [ds]
  (->> (ds/columns ds)
       (map meta)
       (dataset)))

;;;;;;;;;;;;;
;; GROUPING
;;;;;;;;;;;;;

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

(defn- find-group-indexes
  "Calulate indexes for groups"
  [grouping-selector ds limit-columns]
  (cond
    (map? grouping-selector) grouping-selector
    (sequential+? grouping-selector) (ds/group-by->indexes identity grouping-selector ds)
    (fn? grouping-selector) (ds/group-by->indexes grouping-selector limit-columns ds)
    :else (ds/group-by-column->indexes grouping-selector ds)))

(defn- group-indexes->map
  "Create map representing grouped dataset from indexes"
  [ds id [k idxs]]
  {:name k
   :group-id id
   :count (count idxs)
   :data (ds/set-dataset-name (ds/select ds :all idxs) k)})

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

(defn- process-group-data
  [ds f]
  #_(ds/column-map ds :data f :data)
  (ds/add-or-update-column ds :data (map f (ds :data))))

(defn- correct-group-count
  "Correct count for each group"
  [ds]
  (ds/column-map ds :count ds/row-count :data))

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
       (concat col1 col2)
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
    [(col/new-column :$group-id (const-rdr/make-const-reader group-id :int64 count))]))

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
            (reduce ds/concat))
       (ds/set-dataset-name dataset-name))))

;;;;;;;;;;;;;;;;;;;;;;;
;; COLUMNS OPERATIONS
;;;;;;;;;;;;;;;;;;;;;;;

(defn- filter-column-names
  "Filter column names"
  [ds cols-selector meta-field]
  (let [field-fn (if (= :all meta-field)
                   identity
                   (or meta-field :name))]
    (->> ds
         (ds/columns)
         (map meta)
         (filter (comp cols-selector field-fn))
         (map :name))))

(defn- select-column-names
  ([ds cols-selector] (select-column-names ds cols-selector :name))
  ([ds cols-selector meta-field]
   (cond
     (= :all cols-selector) (ds/column-names ds)
     (map? cols-selector) (keys cols-selector)
     (sequential+? cols-selector) cols-selector
     (instance? java.util.regex.Pattern cols-selector) (filter-column-names ds #(re-matches cols-selector (str %)) meta-field)
     (fn? cols-selector) (filter-column-names ds cols-selector meta-field)
     :else [cols-selector])))

(defn- select-or-drop-columns
  "Select or drop columns."
  ([f ds] (select-or-drop-columns f ds :all))
  ([f ds cols-selector] (select-or-drop-columns f ds cols-selector nil))
  ([f ds cols-selector {:keys [meta-field]}]
   (if (grouped? ds)
     (process-group-data ds #(select-or-drop-columns f % cols-selector))
     (f ds (select-column-names ds cols-selector meta-field)))))

(defn- select-or-drop-colums-docstring
  [op]
  (str op " columns by (returns dataset):

  - name
  - sequence of names
  - map of names with new names (rename)
  - function which filter names (via column metadata)"))

(def ^{:doc (select-or-drop-colums-docstring "Select")}
  select-columns (partial select-or-drop-columns ds/select-columns))

(def ^{:doc (select-or-drop-colums-docstring "Drop")}
  drop-columns (partial select-or-drop-columns ds/drop-columns))

(defn rename-columns
  "Rename columns with provided old -> new name map"
  [ds col-map]
  (if (grouped? ds)
    (process-group-data ds #(ds/rename-columns % col-map))
    (ds/rename-columns ds col-map)))

(defn- fix-column-size
  [col-or-seq strategy cnt]
  (let [seq-cnt (count col-or-seq)]
    (cond
      (> seq-cnt cnt) (take cnt col-or-seq)
      (< seq-cnt cnt) (if (= strategy :cycle)
                        (take cnt (cycle col-or-seq))
                        (if (col/is-column? col-or-seq)
                          (col/extend-column-with-empty col-or-seq (- cnt seq-cnt))
                          (concat col-or-seq (repeat (- cnt seq-cnt) nil))))
      :else col-or-seq)))

(defn add-or-update-column
  "Add or update (modify) column under `col-name`.

  `column-seq-or-gen` can be sequence of values or generator function (which gets `ds` as input)."
  ([ds col-name column-seq-or-gen] (add-or-update-column ds col-name column-seq-or-gen nil))
  ([ds col-name column-seq-or-gen {:keys [count-strategy]
                                   :or {count-strategy :cycle}
                                   :as options}]
   (if (grouped? ds)

     (process-group-data ds #(add-or-update-column % col-name column-seq-or-gen options))
     
     (cond
       (or (col/is-column? column-seq-or-gen)
           (sequential+? column-seq-or-gen)) (->> (ds/row-count ds)
                                                  (fix-column-size column-seq-or-gen count-strategy)
                                                  (ds/add-or-update-column ds col-name))
       (fn? column-seq-or-gen) (add-or-update-column ds col-name (column-seq-or-gen ds))
       :else (ds/add-or-update-column ds col-name (repeat (ds/row-count ds) column-seq-or-gen))))))

(defn add-or-update-columns
  "Add or updade (modify) columns defined in `columns-map` (mapping: name -> column-seq-or-gen) "
  ([ds columns-map] (add-or-update-columns ds columns-map nil))
  ([ds columns-map options]
   (reduce-kv (fn [ds k v] (add-or-update-column ds k v options)) ds columns-map)))

(defn map-columns
  ([ds target-column map-fn cols-selector] (map-columns ds target-column map-fn cols-selector nil))
  ([ds target-column map-fn cols-selector {:keys [meta-field]
                                           :as options}]
   (if (grouped? ds)
     (process-group-data ds #(map-columns % target-column map-fn cols-selector options))
     (apply ds/column-map ds target-column map-fn (select-column-names ds cols-selector meta-field)))))

(defn- try-convert-to-type
  [col new-type]
  (col/new-column (col/column-name col) (dtype/->reader col new-type)
                  (meta col) (col/missing col)))

(defn convert-column-type
  "Convert type of the column to the other type."
  ([ds coltype-map]
   (reduce (fn [ds [colname new-type]]
             (convert-column-type ds colname new-type)) ds coltype-map))
  ([ds colname new-type]
   (if (grouped? ds)
     (process-group-data ds #(convert-column-type % colname new-type))
     (ds/add-or-update-column ds colname
                              (let [col (ds colname)]
                                (condp = (dtype/get-datatype col)
                                  :string (col/parse-column new-type col)
                                  :object (if (string? (dtype/get-value col 0))
                                            ;; below doesn't work well
                                            (col/parse-column new-type (try-convert-to-type col :string))
                                            (try-convert-to-type col new-type))
                                  (try-convert-to-type col new-type)))))))

;;;;;;;;;;;;;;;;;;;;
;; ROWS OPERATIONS
;;;;;;;;;;;;;;;;;;;;

(defn- find-indexes-from-seq
  "Find row indexes based on true/false values or indexes"
  [ds rows-selector]
  (if (number? (first rows-selector))
    rows-selector
    (->> rows-selector
         (take (ds/row-count ds))
         (map-indexed vector)
         (filter second)
         (map first))))

(defn- find-indexes-from-fn
  "Filter rows"
  [ds rows-selector limit-columns]
  (->> (or limit-columns :all)
       (ds/select-columns ds)
       (ds/mapseq-reader)
       (map-indexed vector)
       (filter (comp rows-selector second))
       (map first)))

(defn- find-indexes
  ([ds rows-selector] (find-indexes ds rows-selector nil))
  ([ds rows-selector limit-columns]
   (cond
     (number? rows-selector) [(long rows-selector)]
     (sequential+? rows-selector) (find-indexes-from-seq ds rows-selector)
     (fn? rows-selector) (find-indexes-from-fn ds rows-selector limit-columns))))

(defn- select-or-drop-rows
  "Select or drop rows."
  ([f ds rows-selector] (select-or-drop-rows f ds rows-selector nil))
  ([f ds rows-selector {:keys [limit-columns pre]
                        :as options}]
   (if (grouped? ds)
     (let [pre-ds (map #(add-or-update-columns % pre) (ds :data))
           indices (map #(find-indexes % rows-selector limit-columns) pre-ds)]
       (-> ds
           (ds/add-or-update-column :data (map #(select-or-drop-rows f %1 %2) (ds :data) indices))
           (correct-group-count)))
     
     (f ds (find-indexes (add-or-update-columns ds pre) rows-selector limit-columns)))))

(defn- select-or-drop-rows-docstring
  [op]
  (str op " rows using:

  - row id
  - seq of row ids
  - seq of true/false
  - fn with predicate"))

(def ^{:doc (select-or-drop-rows-docstring "Select")}
  select-rows (partial select-or-drop-rows ds/select-rows))

(def ^{:doc (select-or-drop-rows-docstring "Drop")}
  drop-rows (partial select-or-drop-rows ds/drop-rows))

;;;;;;;;;;;;;;;;;;;;;;;;
;; COMBINED OPERATIONS
;;;;;;;;;;;;;;;;;;;;;;;;

(defn- select-or-drop
  "Select columns and rows"
  [fc fs ds cols-selector rows-selector]
  (let [ds (if (and cols-selector
                    (not= :all cols-selector))
             (fc ds cols-selector)
             ds)]
    (if (and rows-selector
             (not= :all rows-selector))
      (fs ds rows-selector)
      ds)))

(def select (partial select-or-drop select-columns select-rows))
(def drop (partial select-or-drop drop-columns drop-rows))

;;;;;;;;;;;;;;;;
;; AGGREGATING
;;;;;;;;;;;;;;;;

(defn- add-agg-result
  [tot-res k agg-res]
  (cond
    (map? agg-res) (reduce conj tot-res agg-res)
    (sequential+? agg-res) (->> agg-res
                                (map-indexed vector)
                                (reduce (fn [b [id v]]
                                          (conj b [(keyword (str (name k) "-" id)) v])) tot-res))
    :else (conj tot-res [k agg-res])))

(defn aggregate
  "Aggregate dataset by providing:

  - aggregation function
  - map with column names and functions
  - sequence of aggregation functions

  Aggregation functions can return:
  - single value
  - seq of values
  - map of values with column names"
  ([ds fn-map-or-seq] (aggregate ds fn-map-or-seq nil))
  ([ds fn-map-or-seq {:keys [default-column-name-prefix]
                      :or {default-column-name-prefix "summary"}
                      :as options}]
   (if (grouped? ds)
     (-> ds
         (ds/add-or-update-column :data (map #(aggregate % fn-map-or-seq options) (ds :data)))
         (correct-group-count)
         (ungroup {:add-group-as-column? true}))
     (cond
       (fn? fn-map-or-seq) (aggregate ds {:summary fn-map-or-seq})
       (sequential+? fn-map-or-seq) (->> fn-map-or-seq
                                         (zipmap (map #(->> %
                                                            (str default-column-name-prefix "-")
                                                            keyword) (range)))
                                         (aggregate ds))
       :else (dataset (reduce (fn [res [k f]]
                                (add-agg-result res k (f ds))) [] fn-map-or-seq) options)))))


;;;;;;;;;;;;;
;; ORDERING
;;;;;;;;;;;;;

(defn- comparator->fn
  [c]
  (cond
    (fn? c) c
    (= :desc c) (comp - compare)
    :else compare))

(defn asc-desc-comparator
  [orders]
  (if-not (sequential+? orders)
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
  ([ds cols-selector]
   (order-by ds cols-selector (if (sequential+? cols-selector)
                                (repeat (count cols-selector) :asc)
                                [:asc])))
  ([ds cols-selector order-or-comparators]
   (if (grouped? ds)

     (process-group-data ds #(order-by % cols-selector order-or-comparators))

     (let [comp-fn (asc-desc-comparator order-or-comparators)]
       (cond
         (sequential+? cols-selector) (ds/sort-by (apply juxt (map #(if (fn? %)
                                                                      %
                                                                      (fn [ds] (get ds %))) cols-selector)) comp-fn ds)
         (fn? cols-selector) (ds/sort-by cols-selector
                                         comp-fn
                                         ds)
         :else (ds/sort-by-column cols-selector comp-fn ds))))))

;;;;;;;;;;;
;; UNIQUE
;;;;;;;;;;;

(defn- strategy-first [k idxs] (first idxs))
(defn- strategy-last [k idxs] (last idxs))
(defn- strategy-random [k idxs] (rand-nth idxs))

(def ^:private strategies
  {:first strategy-first
   :last strategy-last
   :random strategy-random})

(defn- strategy-vectorize
  [ds group-by-names]
  (let [target-names (select-column-names ds (complement (set group-by-names)))]
    (-> (group-by ds group-by-names)
        (process-group-data (fn [ds]
                              (as-> ds ds
                                (select-columns ds target-names)
                                (dataset [(zipmap target-names (map vec (ds/columns ds)))]))))
        (correct-group-count)
        (ungroup {:add-group-as-column? true}))))

(defn unique-by
  ([ds] (unique-by ds (ds/column-names ds)))
  ([ds cols-selector] (unique-by ds cols-selector nil))
  ([ds cols-selector {:keys [strategy limit-columns]
                      :or {strategy :first}
                      :as options}]
   (if (grouped? ds)
     (-> ds
         (ds/add-or-update-column :data (map #(unique-by % cols-selector options) (ds :data)))
         (correct-group-count))

     (if (= 1 (ds/row-count ds))
       ds
       (if (= strategy :vectorize)
         (strategy-vectorize ds (select-column-names ds cols-selector))
         (let [local-options {:keep-fn (get strategies strategy :first)}]
           (cond
             (sequential+? cols-selector) (ds/unique-by identity (assoc local-options :column-name-seq cols-selector) ds)
             (fn? cols-selector) (ds/unique-by cols-selector (if limit-columns
                                                               (assoc local-options :column-name-seq limit-columns)
                                                               local-options) ds)
             :else (ds/unique-by-column cols-selector local-options ds))))))))

;;;;;;;;;;;;
;; MISSING
;;;;;;;;;;;;

(defn select-or-drop-missing
  "Select rows with missing values"
  ([f ds] (select-or-drop-missing f ds nil))
  ([f ds cols-selector]
   (if (grouped? ds)
     (-> ds
         (ds/add-or-update-column :data (map #(select-or-drop-missing f % cols-selector) (ds :data)))
         (correct-group-count))
     (let [ds- (if cols-selector
                 (select-columns ds cols-selector)
                 ds)]
       (f ds (ds/missing ds-))))))

(defn- select-or-drop-missing-docstring
  [op]
  (str op " rows with missing values

 `cols-selector` selects columns to look at missing values"))

(def ^{:doc (select-or-drop-missing-docstring "Select")}
  select-missing (partial select-or-drop-missing ds/select-rows))

(def ^{:doc (select-or-drop-missing-docstring "Drop")}
  drop-missing (partial select-or-drop-missing ds/drop-rows))

;;;;;;;;;;;;;;;
;; JOIN/SPLIT
;;;;;;;;;;;;;;;

(defn join-columns
  ([ds target-column cols-selector] (join-columns ds target-column cols-selector nil))
  ([ds target-column cols-selector {:keys [separator missing-subst drop-columns? result-type]
                                    :or {separator "-" drop-columns? false result-type :string}
                                    :as options}]
   (if (grouped? ds)
     
     (process-group-data ds #(join-columns % target-column cols-selector options))

     (let [cols (select-columns ds cols-selector)
           missing-subst-fn #(map (fn [row] (or row missing-subst)) %)
           join-function (condp = result-type
                           :map (let [col-names (ds/column-names cols)]
                                  #(zipmap col-names %))
                           :seq seq
                           (let [sep (if (sequential+? separator)
                                       (cycle separator)
                                       (repeat separator))]
                             (fn [row] (->> row
                                           (interleave sep)
                                           (rest)
                                           (apply str)))))]
       
       (let [result (ds/add-or-update-column ds target-column (->> (ds/value-reader cols options)
                                                                   (map (comp join-function missing-subst-fn))))]

         (if drop-columns? (drop-columns result cols-selector) result))))))

(defn separate-column
  ([ds column target-columns separator] (separate-column ds column target-columns separator nil))
  ([ds column target-columns separator {:keys [missing-subst drop-column?]
                                        :or {drop-column? false}
                                        :as options}]
   (if (grouped? ds)
     
     (process-group-data ds #(separate-column % column target-columns separator options))

     (let [separator-fn (cond
                          (string? separator) (let [pat (re-pattern separator)]
                                                #(str/split (str %) pat))
                          (instance? java.util.regex.Pattern separator) #(rest (re-matches separator (str %)))
                          :else separator)
           replace-missing (if missing-subst
                             (let [f (if (or (set? missing-subst)
                                             (fn? missing-subst))
                                       missing-subst
                                       (partial = missing-subst))]
                               (fn [res]
                                 (map #(if (f %) nil %) res)))
                             identity)
           result (->> (ds column)
                       (map (comp (partial zipmap target-columns) replace-missing separator-fn))
                       (dataset)
                       (ds/columns)
                       (reduce (partial ds/add-column) ds))]
       (if drop-column? (drop-columns result column) result)))))


;;;;;;;;;;;;
;; RESHAPE
;;;;;;;;;;;;

(defn- regroup-cols-from-template
  [ds cols names value-name column-split-fn]
  (let [template? (some nil? names)
        pre-groups (->> cols
                        (map (fn [col-name]
                               (let [col (ds col-name)
                                     split (column-split-fn col-name)
                                     buff (if template? {} {value-name col})]
                                 (into buff (mapv (fn [k v] (if k [k v] [v col]))
                                                  names split))))))
        groups (-> (->> names
                        (remove nil?)
                        (map #(fn [n] (get n %)))
                        (apply juxt))
                   (clojure.core/group-by pre-groups)
                   (vals))]
    (map #(reduce merge %) groups)))

(defn- cols->pre-longer
  ([ds cols names value-name]
   (cols->pre-longer ds cols names value-name nil))
  ([ds cols names value-name column-splitter]
   (let [column-split-fn (cond (instance? java.util.regex.Pattern column-splitter) (comp rest #(re-find column-splitter (str %)))
                               (fn? column-splitter) column-splitter
                               :else vector)
         names (if (sequential+? names) names [names])]
     (regroup-cols-from-template ds cols names value-name column-split-fn))))

(defn- pre-longer->target-cols
  [ds cnt m]
  (let [new-cols (map (fn [[col-name maybe-column]]
                        (if (col/is-column? maybe-column)
                          (col/set-name maybe-column col-name)
                          (col/new-column col-name (const-rdr/make-const-reader maybe-column :object cnt)))) m)]
    (ds/append-columns ds new-cols)))

(defn pivot->longer
  "`tidyr` pivot_longer api"
  ([ds cols-selector] (pivot->longer ds cols-selector nil))
  ([ds cols-selector {:keys [target-cols value-column-name splitter drop-missing? meta-field datatypes]
                      :or {target-cols :$column
                           value-column-name :$value
                           drop-missing? true}}]
   (let [cols (select-column-names ds cols-selector meta-field)
         groups (cols->pre-longer ds cols target-cols value-column-name splitter)
         ds-template (drop-columns ds cols)
         cnt (ds/row-count ds-template)]
     (as-> (ds/set-metadata (->> groups                                        
                                 (map (partial pre-longer->target-cols ds-template cnt))
                                 (reduce ds/concat))
                            (ds/metadata ds)) final-ds
       (if drop-missing? (drop-missing final-ds (keys (first groups))) final-ds)
       (if datatypes (convert-column-type final-ds datatypes) final-ds)))))

(defn pivot->wider
  [ds cols-selector value-columns]
  (let [col-names (select-column-names ds cols-selector)
        value-names (select-column-names ds value-columns)
        single-value? (= (count value-names) 1)
        rest-cols (->> (concat col-names value-names)
                       (set)
                       (complement)
                       (select-column-names ds))
        join-on-single? (= (count rest-cols) 1)
        join-name (if join-on-single?
                    (first rest-cols)
                    (gensym (apply str "^____" rest-cols)))
        col-to-drop (if join-on-single?
                      (str "right." join-name)
                      (symbol (str "right." join-name)))
        pre-ds (if join-on-single?
                 ds
                 (join-columns ds join-name rest-cols {:result-type :seq
                                                       :drop-columns? true}))
        starting-ds (unique-by (select-columns pre-ds join-name))
        starting-ds-count (ds/row-count starting-ds)
        grouped-ds (group-by pre-ds col-names)
        group-name->names (->> col-names
                               (map #(fn [m] (get m %)))
                               (apply juxt))
        result (reduce (fn [curr-ds {:keys [name data]}]
                         (let [col-name (str/join "_" (group-name->names name))
                               target-names (if single-value?
                                              [col-name]
                                              (map #(str % "-" col-name) value-names))
                               rename-map (zipmap value-names target-names)
                               data (-> data
                                        (rename-columns rename-map)
                                        (select-columns (conj target-names join-name))
                                        (->> (ds/left-join join-name curr-ds))
                                        (drop-columns col-to-drop))]
                           (if (> (ds/row-count data) starting-ds-count)
                             (strategy-vectorize data (select-column-names data (complement (set target-names))))
                             data)))
                       starting-ds
                       (reverse (ds/mapseq-reader grouped-ds)))]
    (-> (if join-on-single?
          result
          (let [temp-ds (separate-column result join-name rest-cols identity {:drop-column? true})
                rest-ds (select-columns temp-ds rest-cols)]
            (reduce #(ds/add-column %1 %2) rest-ds (ds/columns (drop-columns temp-ds rest-cols)))))
        (ds/set-dataset-name (ds/dataset-name ds)))))

;;;;;;;;;;;;;;
;; USE CASES
;;;;;;;;;;;;;;

(dataset 999)
(dataset [[:A 33]])
(dataset {:A 33})
(dataset {:A [1 2 3]})
(dataset {:A [3 4 5] :B "X"})
(dataset [[:A [3 4 5]] [:B "X"]])
(dataset [{:a 1 :b 3} {:b 2 :a 99}])

(dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}])
(dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}] {:unroll {:b :float64}})
(dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}] {:unroll [:b]})
(dataset [{:a 1 :b [1 2 3] :c 2} {:a 2 :b [3 4] :c 1} {:a 3 :b 3 :c [2 3]}] {:unroll [:b :c]})

;; unroll

(unroll (dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}]) :b)
(unroll (dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}]) [:b])
(unroll (dataset [{:a 1 :b [1 2 3]} {:a 2 :b [3 4]}]) {:b :float32})


;; https://archive.ics.uci.edu/ml/machine-learning-databases/kddcup98-mld/epsilon_mirror/
;; (time (def ds (dataset "cup98LRN.txt.gz")))
(def DS (dataset {:V1 (take 9 (cycle [1 2]))
                  :V2 (range 1 10)
                  :V3 (take 9 (cycle [0.5 1.0 1.5]))
                  :V4 (take 9 (cycle [\A \B \C]))}))

(def DSm (dataset {:V1 (take 9 (cycle [1 2 nil]))
                   :V2 (range 1 10)
                   :V3 (take 9 (cycle [0.5 1.0 nil 1.5]))
                   :V4 (take 9 (cycle [\A \B \C]))}))

(group-by DS :V1 {:result-type :as-map})
(group-by DS [:V1 :V3])
(group-by DS (juxt :V1 :V4))
(group-by DS #(< (:V2 %) 4))
(group-by DS (comp #{\B} :V4))

(grouped? DS)
(grouped? (group-by DS [:V1 :V3]))

(ungroup (group-by DS :V1) {:dataset-name "ungrouped"})
(ungroup (group-by DS [:V1 :V3]) {:add-group-id-as-column? true})
(ungroup (group-by DS (juxt :V1 :V4)) {:add-group-as-column? true})
(ungroup (group-by DS #(< (:V2 %) 4)) {:add-group-as-column? true})
(ungroup (group-by DS (comp #{\B \C} :V4)) {:add-group-as-column? true})

(group-as-map (group-by DS (juxt :V1 :V4)))

(select-columns DS :V1)
(select-columns DS [:V1 :V2])
(select-columns DS {:V1 "v1"
                    :V2 "v2"})
(select-columns DS #(= :int64 %) {:meta-field :datatype})

(drop-columns DS :V1)
(drop-columns DS [:V1 :V2])
(drop-columns DS #(= :int64 %) {:meta-field :datatype})

(rename-columns DS {:V1 "v1"
                    :V2 "v2"})

(ungroup (select-columns (group-by DS :V1) {:V1 "v1"}))
(ungroup (drop-columns (group-by DS [:V4]) (comp #{:int64} :datatype)))

(add-or-update-column DS :abc [1 2] {:count-strategy :na})
(add-or-update-column DS :abc [1 2] {:count-strategy :cycle})
(add-or-update-column DS :abc "X")
(add-or-update-columns DS {:abc "X"
                           :xyz [1 2 3]})
(add-or-update-column DS :abc (fn [ds] (dfn/+ (ds :V1) (ds :V2))))

(add-or-update-columns DS {:abc "X"
                           :xyz [1 2 3]} {:count-strategy :na})

(ungroup (add-or-update-column (group-by DS :V4) :abc #(let [mean (dfn/mean (% :V2))
                                                                stddev (dfn/standard-deviation (% :V2))]
                                                            (dfn// (dfn/- (% :V2) mean)
                                                                   stddev))))


(ungroup (add-or-update-columns (group-by DS :V4) {:abc "X"
                                                      :xyz [-1]} {:count-strategy :na}))

(convert-column-type DS :V1 :float64)

(select-rows DS 3)
(drop-rows DS 3)

(select-rows DS [3 4 7])
(drop-rows DS [3 4 7])

(select-rows DS (comp #(< 1 %) :V1))
(drop-rows DS (comp #(< 1 %) :V1))

(select-rows DS [true nil nil true])
(drop-rows DS [true nil nil true])

(ungroup (select-rows (group-by DS :V1) 0))
(ungroup (drop-rows (group-by DS :V1) 0))

;; select rows where :V2 values are lower than column mean
(let [mean (dfn/mean (DS :V2))]
  (select-rows DS (fn [row] (< (:V2 row) mean))))

;; select rows of grouped (by :V4) where :V2 values are lower than :V2 mean.
;; pre option adds temporary columns according to provided map before row selecting 
(ungroup (select-rows (group-by DS :V4)
                      (fn [row] (< (:V2 row) (:mean row)))
                      {:pre {:mean #(dfn/mean (% :V2))}}))

(aggregate DS [#(dfn/mean (% :V3)) (fn [ds] {:mean-v1 (dfn/mean (ds :V1))
                                            :mean-v2 (dfn/mean (ds :V2))})])

(aggregate (group-by DS :V4) [#(dfn/mean (% :V3)) (fn [ds] {:mean-v1 (dfn/mean (ds :V1))
                                                              :mean-v2 (dfn/mean (ds :V2))})])

(order-by DS :V1)
(order-by DS :V1 :desc)

(order-by DS [:V1 :V2])
(order-by DS [:V1 :V2] [:asc :desc])
(order-by DS [:V1 :V2] [:desc :desc])
(order-by DS [:V3 :V1] [:desc :asc])

(order-by DS [:V4 (fn [row] (* (:V1 row)
                              (:V2 row)
                              (:V3 row)))] [#(if (= %1 %2) -1 0) :asc])

(ungroup (order-by (group-by DS :V4) [:V1 :V2] [:desc :desc]))

(unique-by DS)

(unique-by DS :V1)
(unique-by DS :V1 {:strategy :last})
(unique-by DS :V1 {:strategy :random})
(unique-by DS :V1 {:strategy :vectorize})

(unique-by DS [:V1 :V4])
(unique-by DS [:V1 :V4] {:strategy :last})

(unique-by DS (fn [m] (mod (:V2 m) 3)))
(unique-by DS (fn [m] (mod (:V2 m) 3)) {:strategy :last :limit-columns [:V2]})

(ungroup (unique-by (group-by DS :V4) [:V1 :V3]))

(select-missing DSm)
(drop-missing DSm)

(select-missing DSm :V1)
(drop-missing DSm :V1)

(ungroup (select-missing (group-by DSm :V4)))
(ungroup (drop-missing (group-by DSm :V4)))


(join-columns DSm :joined [:V1 :V2 :V4])
(join-columns DSm :joined [:V1 :V2 :V4] {:drop-columns? true})
(join-columns DSm :joined [:V1 :V2 :V4] {:separator "!" :missing-subst "NA"})
(join-columns DSm :joined :all {:separator ["|" "-"] :missing-subst "NA"})
(ungroup (join-columns (group-by DSm :V4) :joined [:V1 :V2 :V4]))
(ungroup (join-columns (group-by DSm :V4) :joined [:V1 :V2 :V4] {:drop-columns? true}))
(ungroup (join-columns (group-by DSm :V2) :joined [:V1 :V2 :V4] {:separator "!" :missing-subst "NA"}))

(join-columns DSm :joined [:V1 :V2 :V4] {:result-type :map})
(join-columns DSm :joined [:V1 :V2 :V4] {:result-type :seq})


(separate-column DS :V3 [:int-part :frac-part] (fn [^double v]
                                                 [(int (quot v 1.0))
                                                  (mod v 1.0)]))

(separate-column DS :V3 [:int-part :frac-part] (fn [^double v]
                                                 [(int (quot v 1.0))
                                                  (mod v 1.0)]) {:drop-column? true})

(separate-column DS :V3 [:int-part :frac-part] (fn [^double v]
                                                 [(int (quot v 1.0))
                                                  (mod v 1.0)]) {:drop-column? true
                                                                 :missing-subst #{0 0.0}})

(separate-column (join-columns DSm :joined [:V1 :V2 :V4] {:result-type :map})
                 :joined [:v1 :v2 :v4] (juxt :V1 :V2 :V4))

(separate-column (join-columns DSm :joined [:V1 :V2 :V4] {:result-type :seq})
                 :joined [:v1 :v2 :v4] identity)


;;;;

;; private tests

(find-indexes DS 1)
(find-indexes DS [2 3])
(find-indexes DS [true nil nil true])
(find-indexes DS (comp #(< 1 %) :V1))

;;;;

(defonce flights (dataset "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv"))

(-> flights ;; take dataset (loaded from the net
    (drop-missing) ;; remove rows with missing values
    (select-rows #(= "AA" (get % "carrier"))) ;; filter rows by carrier="AA"
    (group-by ["origin" "dest" "month"]) ;; group by several columns
    (aggregate {:arr-delay-mean #(dfn/mean (% "arr_delay")) ;; calculate mean of arr_delay...
                :dep-delay-mean #(dfn/mean (% "dep_delay"))}) ;; ...and dep_delay
    (order-by ["origin" "dest" "month"]) ;; order by some columns
    (select-rows (range 12)) ;; take first 12 rows
    )
;; => _unnamed [12 5]:
;;    | month | origin | dest | :arr-delay-mean | :dep-delay-mean |
;;    |-------+--------+------+-----------------+-----------------|
;;    | 1.000 |    EWR |  DFW |           6.428 |           10.01 |
;;    | 2.000 |    EWR |  DFW |           10.54 |           11.35 |
;;    | 3.000 |    EWR |  DFW |           12.87 |           8.080 |
;;    | 4.000 |    EWR |  DFW |           17.79 |           12.92 |
;;    | 5.000 |    EWR |  DFW |           18.49 |           18.68 |
;;    | 6.000 |    EWR |  DFW |           37.01 |           38.74 |
;;    | 7.000 |    EWR |  DFW |           20.25 |           21.15 |
;;    | 8.000 |    EWR |  DFW |           16.94 |           22.07 |
;;    | 9.000 |    EWR |  DFW |           5.865 |           13.06 |
;;    | 10.00 |    EWR |  DFW |           18.81 |           18.89 |
;;    | 1.000 |    EWR |  LAX |           1.367 |           7.500 |
;;    | 2.000 |    EWR |  LAX |           10.33 |           4.111 |


;;; stocks aggregation

(defonce stocks (dataset "https://raw.githubusercontent.com/techascent/tech.ml.dataset/master/test/data/stocks.csv" {:key-fn keyword}))

(-> stocks
    (group-by (fn [row]
                {:symbol (:symbol row)
                 :year (dtype-dt-ops/get-years (:date row))}))
    (aggregate #(dfn/mean (% :price)))
    (order-by [:symbol :year])
    (select-rows (range 12)))
;; => _unnamed [12 3]:
;;    | :symbol | :year | :summary |
;;    |---------+-------+----------|
;;    |    AAPL |  2000 |    21.75 |
;;    |    AAPL |  2001 |    10.18 |
;;    |    AAPL |  2002 |    9.408 |
;;    |    AAPL |  2003 |    9.347 |
;;    |    AAPL |  2004 |    18.72 |
;;    |    AAPL |  2005 |    48.17 |
;;    |    AAPL |  2006 |    72.04 |
;;    |    AAPL |  2007 |    133.4 |
;;    |    AAPL |  2008 |    138.5 |
;;    |    AAPL |  2009 |    150.4 |
;;    |    AAPL |  2010 |    206.6 |
;;    |    AMZN |  2000 |    43.93 |

(-> stocks
    (group-by (juxt :symbol #(dtype-dt-ops/get-years (% :date))))
    (aggregate #(dfn/mean (% :price)))
    (order-by :$group-name)
    (select-rows (range 12)))
;; => _unnamed [12 2]:
;;    |  :$group-name | :summary |
;;    |---------------+----------|
;;    | ["AAPL" 2000] |    21.75 |
;;    | ["AAPL" 2001] |    10.18 |
;;    | ["AAPL" 2002] |    9.408 |
;;    | ["AAPL" 2003] |    9.347 |
;;    | ["AAPL" 2004] |    18.72 |
;;    | ["AAPL" 2005] |    48.17 |
;;    | ["AAPL" 2006] |    72.04 |
;;    | ["AAPL" 2007] |    133.4 |
;;    | ["AAPL" 2008] |    138.5 |
;;    | ["AAPL" 2009] |    150.4 |
;;    | ["AAPL" 2010] |    206.6 |
;;    | ["AMZN" 2000] |    43.93 |

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; RESHAPE TESTS

;; TESTS

(def data (dataset "data/relig_income.csv"))
(pivot->longer data (complement #{"religion"}))
;; => data/relig_income.csv [180 3]:
;;    |                religion | :$value |           :$column |
;;    |-------------------------+---------+--------------------|
;;    |                Agnostic |      27 |              <$10k |
;;    |                 Atheist |      12 |              <$10k |
;;    |                Buddhist |      27 |              <$10k |
;;    |                Catholic |     418 |              <$10k |
;;    |      Don’t know/refused |      15 |              <$10k |
;;    |        Evangelical Prot |     575 |              <$10k |
;;    |                   Hindu |       1 |              <$10k |
;;    | Historically Black Prot |     228 |              <$10k |
;;    |       Jehovah's Witness |      20 |              <$10k |
;;    |                  Jewish |      19 |              <$10k |
;;    |           Mainline Prot |     289 |              <$10k |
;;    |                  Mormon |      29 |              <$10k |
;;    |                  Muslim |       6 |              <$10k |
;;    |                Orthodox |      13 |              <$10k |
;;    |         Other Christian |       9 |              <$10k |
;;    |            Other Faiths |      20 |              <$10k |
;;    |   Other World Religions |       5 |              <$10k |
;;    |            Unaffiliated |     217 |              <$10k |
;;    |                Agnostic |      96 | Don't know/refused |


(def data (-> (dataset "data/billboard.csv.gz")
              (drop-columns #(= :boolean %) {:meta-field :datatype}))) ;; drop some boolean columns, tidyr just skips them

(pivot->longer data #(str/starts-with? % "wk") {:target-cols :week
                                                :value-column-name :rank})
;; => data/billboard.csv.gz [5307 5]:
;;    |              artist |                   track | date.entered | :rank | :week |
;;    |---------------------+-------------------------+--------------+-------+-------|
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |     4 |  wk35 |
;;    |       Braxton, Toni |    He Wasn't Man Enough |   2000-03-18 |    34 |  wk35 |
;;    |               Creed |                  Higher |   1999-09-11 |    22 |  wk35 |
;;    |               Creed |     With Arms Wide Open |   2000-05-13 |     5 |  wk35 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |     8 |  wk35 |
;;    |                 Joe |            I Wanna Know |   2000-01-01 |     5 |  wk35 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |    14 |  wk35 |
;;    |    Vertical Horizon |     Everything You Want |   2000-01-22 |    27 |  wk35 |
;;    |     matchbox twenty |                    Bent |   2000-04-29 |    33 |  wk35 |
;;    |               Creed |                  Higher |   1999-09-11 |    21 |  wk55 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |    22 |  wk55 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    18 |  wk19 |
;;    |        3 Doors Down |                   Loser |   2000-10-21 |    73 |  wk19 |
;;    |                98^0 | Give Me Just One Nig... |   2000-08-19 |    93 |  wk19 |
;;    |             Aaliyah |           I Don't Wanna |   2000-01-29 |    83 |  wk19 |
;;    |             Aaliyah |               Try Again |   2000-03-18 |     3 |  wk19 |
;;    |      Adams, Yolanda |           Open My Heart |   2000-08-26 |    79 |  wk19 |
;;    | Aguilera, Christina | Come On Over Baby (A... |   2000-08-05 |    23 |  wk19 |
;;    | Aguilera, Christina |           I Turn To You |   2000-04-15 |    29 |  wk19 |
;;    | Aguilera, Christina |       What A Girl Wants |   1999-11-27 |    18 |  wk19 |
;;    |        Alice Deejay |        Better Off Alone |   2000-04-08 |    79 |  wk19 |
;;    |               Amber |                  Sexual |   1999-07-17 |    95 |  wk19 |
;;    |       Anthony, Marc |             My Baby You |   2000-09-16 |    91 |  wk19 |
;;    |       Anthony, Marc |          You Sang To Me |   2000-02-26 |     9 |  wk19 |
;;    |               Avant |           My First Love |   2000-11-04 |    81 |  wk19 |

(pivot->longer data #(str/starts-with? % "wk") {:target-cols :week
                                                :value-column-name :rank
                                                :splitter #"wk(.*)"
                                                :datatypes {:week :int16}})

;; => data/billboard.csv.gz [5307 5]:
;;    |              artist |                   track | date.entered | :rank | :week |
;;    |---------------------+-------------------------+--------------+-------+-------|
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    21 |    46 |
;;    |               Creed |                  Higher |   1999-09-11 |     7 |    46 |
;;    |               Creed |     With Arms Wide Open |   2000-05-13 |    37 |    46 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |    31 |    46 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |     5 |    46 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    42 |    51 |
;;    |               Creed |                  Higher |   1999-09-11 |    14 |    51 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |    49 |    51 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |    12 |    51 |
;;    |               2 Pac | Baby Don't Cry (Keep... |   2000-02-26 |    94 |     6 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    57 |     6 |
;;    |        3 Doors Down |                   Loser |   2000-10-21 |    65 |     6 |
;;    |            504 Boyz |           Wobble Wobble |   2000-04-15 |    31 |     6 |
;;    |                98^0 | Give Me Just One Nig... |   2000-08-19 |    19 |     6 |
;;    |             Aaliyah |           I Don't Wanna |   2000-01-29 |    35 |     6 |
;;    |             Aaliyah |               Try Again |   2000-03-18 |    18 |     6 |
;;    |      Adams, Yolanda |           Open My Heart |   2000-08-26 |    67 |     6 |
;;    |       Adkins, Trace |                    More |   2000-04-29 |    69 |     6 |
;;    | Aguilera, Christina | Come On Over Baby (A... |   2000-08-05 |    18 |     6 |
;;    | Aguilera, Christina |           I Turn To You |   2000-04-15 |    19 |     6 |
;;    | Aguilera, Christina |       What A Girl Wants |   1999-11-27 |    13 |     6 |
;;    |        Alice Deejay |        Better Off Alone |   2000-04-08 |    36 |     6 |
;;    |               Amber |                  Sexual |   1999-07-17 |    93 |     6 |
;;    |       Anthony, Marc |             My Baby You |   2000-09-16 |    81 |     6 |
;;    |       Anthony, Marc |          You Sang To Me |   2000-02-26 |    27 |     6 |

(def data (dataset "data/who.csv.gz"))

(pivot->longer data #(str/starts-with? % "new") {:target-cols [:diagnosis :gender :age]
                                                 :splitter #"new_?(.*)_(.)(.*)"
                                                 :value-column-name :count})

;; => data/who.csv.gz [76046 8]:
;;    |                           country | iso2 | iso3 | year | :count | :diagnosis | :gender | :age |
;;    |-----------------------------------+------+------+------+--------+------------+---------+------|
;;    |                           Albania |   AL |  ALB | 2013 |     60 |        rel |       m | 1524 |
;;    |                           Algeria |   DZ |  DZA | 2013 |   1021 |        rel |       m | 1524 |
;;    |                           Andorra |   AD |  AND | 2013 |      0 |        rel |       m | 1524 |
;;    |                            Angola |   AO |  AGO | 2013 |   2992 |        rel |       m | 1524 |
;;    |                          Anguilla |   AI |  AIA | 2013 |      0 |        rel |       m | 1524 |
;;    |               Antigua and Barbuda |   AG |  ATG | 2013 |      1 |        rel |       m | 1524 |
;;    |                         Argentina |   AR |  ARG | 2013 |   1124 |        rel |       m | 1524 |
;;    |                           Armenia |   AM |  ARM | 2013 |    116 |        rel |       m | 1524 |
;;    |                         Australia |   AU |  AUS | 2013 |    105 |        rel |       m | 1524 |
;;    |                           Austria |   AT |  AUT | 2013 |     44 |        rel |       m | 1524 |
;;    |                        Azerbaijan |   AZ |  AZE | 2013 |    958 |        rel |       m | 1524 |
;;    |                           Bahamas |   BS |  BHS | 2013 |      2 |        rel |       m | 1524 |
;;    |                           Bahrain |   BH |  BHR | 2013 |     13 |        rel |       m | 1524 |
;;    |                        Bangladesh |   BD |  BGD | 2013 |  14705 |        rel |       m | 1524 |
;;    |                          Barbados |   BB |  BRB | 2013 |      0 |        rel |       m | 1524 |
;;    |                           Belarus |   BY |  BLR | 2013 |    162 |        rel |       m | 1524 |
;;    |                           Belgium |   BE |  BEL | 2013 |     63 |        rel |       m | 1524 |
;;    |                            Belize |   BZ |  BLZ | 2013 |      8 |        rel |       m | 1524 |
;;    |                             Benin |   BJ |  BEN | 2013 |    301 |        rel |       m | 1524 |
;;    |                           Bermuda |   BM |  BMU | 2013 |      0 |        rel |       m | 1524 |
;;    |                            Bhutan |   BT |  BTN | 2013 |    180 |        rel |       m | 1524 |
;;    |  Bolivia (Plurinational State of) |   BO |  BOL | 2013 |   1470 |        rel |       m | 1524 |
;;    | Bonaire, Saint Eustatius and Saba |   BQ |  BES | 2013 |      0 |        rel |       m | 1524 |
;;    |            Bosnia and Herzegovina |   BA |  BIH | 2013 |     57 |        rel |       m | 1524 |
;;    |                          Botswana |   BW |  BWA | 2013 |    423 |        rel |       m | 1524 |

(def data (dataset "data/family.csv"))

(pivot->longer data (complement #{"family"}) {:target-cols [nil :child]
                                              :splitter #(str/split % #"_")
                                              :datatypes {"gender" :int16}})
;; => data/family.csv [9 4]:
;;    | family |        dob | :child | gender |
;;    |--------+------------+--------+--------|
;;    |      1 | 1998-11-26 | child1 |      1 |
;;    |      2 | 1996-06-22 | child1 |      2 |
;;    |      3 | 2002-07-11 | child1 |      2 |
;;    |      4 | 2004-10-10 | child1 |      1 |
;;    |      5 | 2000-12-05 | child1 |      2 |
;;    |      1 | 2000-01-29 | child2 |      2 |
;;    |      3 | 2004-04-05 | child2 |      2 |
;;    |      4 | 2009-08-27 | child2 |      1 |
;;    |      5 | 2005-02-28 | child2 |      1 |

(def data (dataset "data/anscombe.csv"))

(pivot->longer data :all {:splitter #"(.)(.)"
                          :target-cols [nil :set]})
;; => data/anscombe.csv [44 3]:
;;    |  x | :set |     y |
;;    |----+------+-------|
;;    | 10 |    4 | 8.040 |
;;    |  8 |    4 | 6.950 |
;;    | 13 |    4 | 7.580 |
;;    |  9 |    4 | 8.810 |
;;    | 11 |    4 | 8.330 |
;;    | 14 |    4 | 9.960 |
;;    |  6 |    4 | 7.240 |
;;    |  4 |    4 | 4.260 |
;;    | 12 |    4 | 10.84 |
;;    |  7 |    4 | 4.820 |
;;    |  5 |    4 | 5.680 |
;;    | 10 |    4 | 9.140 |
;;    |  8 |    4 | 8.140 |
;;    | 13 |    4 | 8.740 |
;;    |  9 |    4 | 8.770 |
;;    | 11 |    4 | 9.260 |
;;    | 14 |    4 | 8.100 |
;;    |  6 |    4 | 6.130 |
;;    |  4 |    4 | 3.100 |
;;    | 12 |    4 | 9.130 |
;;    |  7 |    4 | 7.260 |
;;    |  5 |    4 | 4.740 |
;;    | 10 |    4 | 7.460 |
;;    |  8 |    4 | 6.770 |
;;    | 13 |    4 | 12.74 |


(def data (dataset {:x [1 2 3 4]
                    :a [1 1 0 0]
                    :b [0 1 1 1]
                    :y1 (repeatedly 4 rand)
                    :y2 (repeatedly 4 rand)
                    :z1 [3 3 3 3]
                    :z2 [-2 -2 -2 -2]}))

(pivot->longer data [:y1 :y2 :z1 :z2] {:target-cols [nil :times]
                                       :splitter #":(.)(.)"})
;; => _unnamed [8 6]:
;;    | :x | :a | :b |       y | :times |  z |
;;    |----+----+----+---------+--------+----|
;;    |  1 |  1 |  0 | 0.04292 |      1 |  3 |
;;    |  2 |  1 |  1 | 0.01423 |      1 |  3 |
;;    |  3 |  0 |  1 | 0.06682 |      1 |  3 |
;;    |  4 |  0 |  1 |  0.7847 |      1 |  3 |
;;    |  1 |  1 |  0 |  0.4265 |      2 | -2 |
;;    |  2 |  1 |  1 |  0.2557 |      2 | -2 |
;;    |  3 |  0 |  1 |  0.6817 |      2 | -2 |
;;    |  4 |  0 |  1 |  0.1968 |      2 | -2 |


(def data (dataset {:id [1 2 3 4]
                    :choice1 ["A" "C" "D" "B"]
                    :choice2 ["B" "B" nil "D"]
                    :choice3 ["C" nil nil nil]}))

(pivot->longer data (complement #{:id}))
;; => _unnamed [8 3]:
;;    | :id | :$value | :$column |
;;    |-----+---------+----------|
;;    |   1 |       A | :choice1 |
;;    |   2 |       C | :choice1 |
;;    |   3 |       D | :choice1 |
;;    |   4 |       B | :choice1 |
;;    |   1 |       B | :choice2 |
;;    |   2 |       B | :choice2 |
;;    |   4 |       D | :choice2 |
;;    |   1 |       C | :choice3 |



;; wider

(def fish (dataset "data/fish_encounters.csv"))

(pivot->wider fish "station" "seen")
;; => data/fish_encounters.csv [19 12]:
;;    | fish | BCW | Lisbon | BCE | BCW2 | MAW | BCE2 | MAE | Release | I80_1 | Base_TD | Rstr |
;;    |------+-----+--------+-----+------+-----+------+-----+---------+-------+---------+------|
;;    | 4842 |   1 |      1 |   1 |    1 |   1 |    1 |   1 |       1 |     1 |       1 |    1 |
;;    | 4843 |   1 |      1 |   1 |    1 |   1 |    1 |   1 |       1 |     1 |       1 |    1 |
;;    | 4844 |   1 |      1 |   1 |    1 |   1 |    1 |   1 |       1 |     1 |       1 |    1 |
;;    | 4845 |     |      1 |     |      |     |      |     |       1 |     1 |       1 |    1 |
;;    | 4848 |     |      1 |     |      |     |      |     |       1 |     1 |         |    1 |
;;    | 4850 |   1 |        |   1 |      |     |      |     |       1 |     1 |       1 |    1 |
;;    | 4855 |     |      1 |     |      |     |      |     |       1 |     1 |       1 |    1 |
;;    | 4857 |   1 |      1 |   1 |    1 |     |    1 |     |       1 |     1 |       1 |    1 |
;;    | 4858 |   1 |      1 |   1 |    1 |   1 |    1 |   1 |       1 |     1 |       1 |    1 |
;;    | 4859 |     |      1 |     |      |     |      |     |       1 |     1 |       1 |    1 |
;;    | 4861 |   1 |      1 |   1 |    1 |   1 |    1 |   1 |       1 |     1 |       1 |    1 |
;;    | 4862 |   1 |      1 |   1 |    1 |     |    1 |     |       1 |     1 |       1 |    1 |
;;    | 4864 |     |        |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4865 |     |      1 |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4847 |     |      1 |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4849 |     |        |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4851 |     |        |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4854 |     |        |     |      |     |      |     |       1 |     1 |         |      |
;;    | 4863 |     |        |     |      |     |      |     |       1 |     1 |         |      |

(def income (dataset "data/us_rent_income.csv"))

(pivot->wider income "variable" ["estimate" "moe"])
;; => data/us_rent_income.csv [52 6]:
;;    | GEOID |                 NAME | estimate-income | moe-income | estimate-rent | moe-rent |
;;    |-------+----------------------+-----------------+------------+---------------+----------|
;;    |     1 |              Alabama |           24476 |        136 |           747 |        3 |
;;    |     2 |               Alaska |           32940 |        508 |          1200 |       13 |
;;    |     4 |              Arizona |           27517 |        148 |           972 |        4 |
;;    |     5 |             Arkansas |           23789 |        165 |           709 |        5 |
;;    |     6 |           California |           29454 |        109 |          1358 |        3 |
;;    |     8 |             Colorado |           32401 |        109 |          1125 |        5 |
;;    |     9 |          Connecticut |           35326 |        195 |          1123 |        5 |
;;    |    10 |             Delaware |           31560 |        247 |          1076 |       10 |
;;    |    11 | District of Columbia |           43198 |        681 |          1424 |       17 |
;;    |    12 |              Florida |           25952 |         70 |          1077 |        3 |
;;    |    13 |              Georgia |           27024 |        106 |           927 |        3 |
;;    |    15 |               Hawaii |           32453 |        218 |          1507 |       18 |
;;    |    16 |                Idaho |           25298 |        208 |           792 |        7 |
;;    |    17 |             Illinois |           30684 |         83 |           952 |        3 |
;;    |    18 |              Indiana |           27247 |        117 |           782 |        3 |
;;    |    19 |                 Iowa |           30002 |        143 |           740 |        4 |
;;    |    20 |               Kansas |           29126 |        208 |           801 |        5 |
;;    |    21 |             Kentucky |           24702 |        159 |           713 |        4 |
;;    |    22 |            Louisiana |           25086 |        155 |           825 |        4 |
;;    |    23 |                Maine |           26841 |        187 |           808 |        7 |
;;    |    24 |             Maryland |           37147 |        152 |          1311 |        5 |
;;    |    25 |        Massachusetts |           34498 |        199 |          1173 |        5 |
;;    |    26 |             Michigan |           26987 |         82 |           824 |        3 |
;;    |    27 |            Minnesota |           32734 |        189 |           906 |        4 |
;;    |    28 |          Mississippi |           22766 |        194 |           740 |        5 |

(def warpbreaks (dataset "data/warpbreaks.csv"))

(pivot->wider (select-columns warpbreaks ["wool" "tension" "breaks"]) "wool" "breaks")
;; => data/warpbreaks.csv [3 3]:
;;    |                            A | tension |                            B |
;;    |------------------------------+---------+------------------------------|
;;    | [18 21 29 17 12 18 35 30 36] |       M | [42 26 19 16 39 28 21 39 29] |
;;    | [26 30 54 25 70 52 51 26 67] |       L | [27 14 29 19 29 31 41 20 44] |
;;    | [36 21 24 18 10 43 28 15 26] |       H | [20 21 24 17 13 15 15 16 28] |

(def production (dataset "data/production.csv"))

(pivot->wider production ["product" "country"] "production")
;; => data/production.csv [15 4]:
;;    | year |     B_AI |    B_EI |     A_AI |
;;    |------+----------+---------+----------|
;;    | 2000 | -0.02618 |   1.405 |    1.637 |
;;    | 2001 |  -0.6886 | -0.5962 |   0.1587 |
;;    | 2002 |  0.06249 | -0.2657 |   -1.568 |
;;    | 2003 |  -0.7234 |  0.6526 |  -0.4446 |
;;    | 2004 |   0.4725 |  0.6256 | -0.07134 |
;;    | 2005 |  -0.9417 |  -1.345 |    1.612 |
;;    | 2006 |  -0.3478 | -0.9718 |  -0.7043 |
;;    | 2007 |   0.5243 |  -1.697 |   -1.536 |
;;    | 2008 |    1.832 | 0.04556 |   0.8391 |
;;    | 2009 |   0.1071 |   1.193 |  -0.3742 |
;;    | 2010 |  -0.3290 |  -1.606 |  -0.7116 |
;;    | 2011 |   -1.783 | -0.7724 |    1.128 |
;;    | 2012 |   0.6113 |  -2.503 |    1.457 |
;;    | 2013 |  -0.7853 |  -1.628 |   -1.559 |
;;    | 2014 |   0.9784 | 0.03330 |  -0.1170 |

(def contacts (dataset "data/contacts.csv"))

(pivot->wider contacts "field" "value")
;; => data/contacts.csv [3 4]:
;;    | person_id | company |             name |           email |
;;    |-----------+---------+------------------+-----------------|
;;    |         2 |  google |       John Smith | john@google.com |
;;    |         1 |  Toyota |   Jiena McLellan |                 |
;;    |         3 |         | Huxley Ratcliffe |                 |

(def world-bank-pop (dataset "data/world_bank_pop.csv.gz"))

(def pop2 (pivot->longer world-bank-pop (map str (range 2000 2018)) {:drop-missing? false
                                                                     :target-cols ["year"]
                                                                     :value-column-name "value"}))
;; => data/world_bank_pop.csv [19008 4]:
;;    | country |   indicator |     value | year |
;;    |---------+-------------+-----------+------|
;;    |     ABW | SP.URB.TOTL | 4.436E+04 | 2013 |
;;    |     ABW | SP.URB.GROW |    0.6695 | 2013 |
;;    |     ABW | SP.POP.TOTL | 1.032E+05 | 2013 |
;;    |     ABW | SP.POP.GROW |    0.5929 | 2013 |
;;    |     AFG | SP.URB.TOTL | 7.734E+06 | 2013 |
;;    |     AFG | SP.URB.GROW |     4.193 | 2013 |
;;    |     AFG | SP.POP.TOTL | 3.173E+07 | 2013 |
;;    |     AFG | SP.POP.GROW |     3.315 | 2013 |
;;    |     AGO | SP.URB.TOTL | 1.612E+07 | 2013 |
;;    |     AGO | SP.URB.GROW |     4.723 | 2013 |
;;    |     AGO | SP.POP.TOTL | 2.600E+07 | 2013 |
;;    |     AGO | SP.POP.GROW |     3.532 | 2013 |
;;    |     ALB | SP.URB.TOTL | 1.604E+06 | 2013 |
;;    |     ALB | SP.URB.GROW |     1.744 | 2013 |
;;    |     ALB | SP.POP.TOTL | 2.895E+06 | 2013 |
;;    |     ALB | SP.POP.GROW |   -0.1832 | 2013 |
;;    |     AND | SP.URB.TOTL | 7.153E+04 | 2013 |
;;    |     AND | SP.URB.GROW |    -2.119 | 2013 |
;;    |     AND | SP.POP.TOTL | 8.079E+04 | 2013 |
;;    |     AND | SP.POP.GROW |    -2.013 | 2013 |
;;    |     ARB | SP.URB.TOTL | 2.186E+08 | 2013 |
;;    |     ARB | SP.URB.GROW |     2.783 | 2013 |
;;    |     ARB | SP.POP.TOTL | 3.817E+08 | 2013 |
;;    |     ARB | SP.POP.GROW |     2.249 | 2013 |
;;    |     ARE | SP.URB.TOTL | 7.661E+06 | 2013 |

(def pop3 (-> (separate-column pop2 "indicator" ["area" "variable"] #(rest (str/split % #"\.")) {:drop-column? true})))
;; => data/world_bank_pop.csv [19008 5]:
;;    | country |     value | year | area | variable |
;;    |---------+-----------+------+------+----------|
;;    |     ABW | 4.436E+04 | 2013 |  URB |     TOTL |
;;    |     ABW |    0.6695 | 2013 |  URB |     GROW |
;;    |     ABW | 1.032E+05 | 2013 |  POP |     TOTL |
;;    |     ABW |    0.5929 | 2013 |  POP |     GROW |
;;    |     AFG | 7.734E+06 | 2013 |  URB |     TOTL |
;;    |     AFG |     4.193 | 2013 |  URB |     GROW |
;;    |     AFG | 3.173E+07 | 2013 |  POP |     TOTL |
;;    |     AFG |     3.315 | 2013 |  POP |     GROW |
;;    |     AGO | 1.612E+07 | 2013 |  URB |     TOTL |
;;    |     AGO |     4.723 | 2013 |  URB |     GROW |
;;    |     AGO | 2.600E+07 | 2013 |  POP |     TOTL |
;;    |     AGO |     3.532 | 2013 |  POP |     GROW |
;;    |     ALB | 1.604E+06 | 2013 |  URB |     TOTL |
;;    |     ALB |     1.744 | 2013 |  URB |     GROW |
;;    |     ALB | 2.895E+06 | 2013 |  POP |     TOTL |
;;    |     ALB |   -0.1832 | 2013 |  POP |     GROW |
;;    |     AND | 7.153E+04 | 2013 |  URB |     TOTL |
;;    |     AND |    -2.119 | 2013 |  URB |     GROW |
;;    |     AND | 8.079E+04 | 2013 |  POP |     TOTL |
;;    |     AND |    -2.013 | 2013 |  POP |     GROW |
;;    |     ARB | 2.186E+08 | 2013 |  URB |     TOTL |
;;    |     ARB |     2.783 | 2013 |  URB |     GROW |
;;    |     ARB | 3.817E+08 | 2013 |  POP |     TOTL |
;;    |     ARB |     2.249 | 2013 |  POP |     GROW |
;;    |     ARE | 7.661E+06 | 2013 |  URB |     TOTL |

(pivot->wider pop3 "variable" "value")
;; => data/world_bank_pop.csv [9504 5]:
;;    | country | year | area |      TOTL |    GROW |
;;    |---------+------+------+-----------+---------|
;;    |     ABW | 2013 |  URB | 4.436E+04 |  0.6695 |
;;    |     ABW | 2013 |  POP | 1.032E+05 |  0.5929 |
;;    |     AFG | 2013 |  URB | 7.734E+06 |   4.193 |
;;    |     AFG | 2013 |  POP | 3.173E+07 |   3.315 |
;;    |     AGO | 2013 |  URB | 1.612E+07 |   4.723 |
;;    |     AGO | 2013 |  POP | 2.600E+07 |   3.532 |
;;    |     ALB | 2013 |  URB | 1.604E+06 |   1.744 |
;;    |     ALB | 2013 |  POP | 2.895E+06 | -0.1832 |
;;    |     AND | 2013 |  URB | 7.153E+04 |  -2.119 |
;;    |     AND | 2013 |  POP | 8.079E+04 |  -2.013 |
;;    |     ARB | 2013 |  URB | 2.186E+08 |   2.783 |
;;    |     ARB | 2013 |  POP | 3.817E+08 |   2.249 |
;;    |     ARE | 2013 |  URB | 7.661E+06 |   1.555 |
;;    |     ARE | 2013 |  POP | 9.006E+06 |   1.182 |
;;    |     ARG | 2013 |  URB | 3.882E+07 |   1.188 |
;;    |     ARG | 2013 |  POP | 4.254E+07 |   1.047 |
;;    |     ARM | 2013 |  URB | 1.828E+06 |  0.2810 |
;;    |     ARM | 2013 |  POP | 2.894E+06 |  0.4013 |
;;    |     ASM | 2013 |  URB | 4.831E+04 | 0.05798 |
;;    |     ASM | 2013 |  POP | 5.531E+04 |  0.1393 |
;;    |     ATG | 2013 |  URB | 2.480E+04 |  0.3838 |
;;    |     ATG | 2013 |  POP | 9.782E+04 |   1.076 |
;;    |     AUS | 2013 |  URB | 1.979E+07 |   1.875 |
;;    |     AUS | 2013 |  POP | 2.315E+07 |   1.758 |
;;    |     AUT | 2013 |  URB | 4.862E+06 |  0.9196 |
