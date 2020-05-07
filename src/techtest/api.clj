(ns techtest.api
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.ml.dataset.pipeline :as pipe]
            [tech.ml.protocols.dataset :as prot])
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
    [(col/new-column :$group-id (repeat count group-id))]))

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
  [cols-selector ds]
  (->> ds
       (ds/columns)
       (map meta)
       (filter cols-selector)
       (map :name)))

(defn- select-or-drop-columns
  "Select or drop columns."
  ([f ds] (select-or-drop-columns f ds :all))
  ([f ds cols-selector]
   (if (grouped? ds)
     (ds/add-or-update-column ds :data (map #(select-or-drop-columns f % cols-selector) (ds :data)))
     (f ds (cond
             (= :all cols-selector) (ds/column-names ds)
             (or (map? cols-selector)
                 (sequential+? cols-selector)) cols-selector
             (fn? cols-selector) (filter-column-names cols-selector ds)
             :else [cols-selector])))))

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
    (ds/add-or-update-column ds :data (map #(ds/rename-columns % col-map) (ds :data)))
    (ds/rename-columns ds col-map)))

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
  "Add or update (modify) column under `col-name`.

  `column-seq-or-gen` can be sequence of values or generator function (which gets `ds` as input)."
  ([ds col-name column-seq-or-gen] (add-or-update-column ds col-name column-seq-or-gen nil))
  ([ds col-name column-seq-or-gen {:keys [count-strategy]
                                   :or {count-strategy :cycle}
                                   :as options}]
   (if (grouped? ds)

     (ds/add-or-update-column ds :data (map #(add-or-update-column % col-name column-seq-or-gen options) (ds :data)))
     
     (cond
       (or (col/is-column? column-seq-or-gen)
           (sequential+? column-seq-or-gen)) (->> ds
                                                  (ds/row-count)
                                                  (fix-column-size column-seq-or-gen count-strategy)
                                                  (ds/add-or-update-column ds col-name))
       (fn? column-seq-or-gen) (add-or-update-column ds col-name (column-seq-or-gen ds))
       :else (ds/add-or-update-column ds col-name (repeat (ds/row-count ds) column-seq-or-gen))))))

(defn add-or-update-columns
  "Add or updade (modify) columns defined in `columns-map` (mapping: name -> column-seq-or-gen) "
  ([ds columns-map] (add-or-update-columns ds columns-map nil))
  ([ds columns-map options]
   (reduce-kv (fn [ds k v] (add-or-update-column ds k v options)) ds columns-map)))

(defn convert-column-type
  "Convert type of the column to the other type.

  Possible types: `:int64` `:int32` `:int16` `:int8` `:float64` `:float32` `:uint64` `:uint32` `:uint16` `:uint8` `:boolean` `:object`"
  [ds colname new-type]
  (ds/add-or-update-column ds colname (dtype/->reader (ds colname) new-type)))

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
       (as-> ds ds
         (ds/add-or-update-column ds :data (map #(select-or-drop-rows f %1 %2) (ds :data) indices))
         (ds/add-or-update-column ds :count (map ds/row-count (ds :data)))))
     
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
     (as-> ds ds
       (ds/add-or-update-column ds :data (map #(aggregate % fn-map-or-seq options) (ds :data)))
       (ds/add-or-update-column ds :count (map ds/row-count (ds :data)))
       (ungroup ds {:add-group-as-column? true}))
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

     (ds/add-or-update-column ds :data (map #(order-by % cols-selector order-or-comparators) (ds :data)))

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

(defn unique-by
  ([ds] (unique-by ds (ds/column-names ds)))
  ([ds row-selector] (unique-by ds row-selector nil))
  ([ds row-selector {:keys [strategy limit-columns]
                     :or {strategy :first}
                     :as options}]
   (if (grouped? ds)
     (as-> ds ds
       (ds/add-or-update-column ds :data (map #(unique-by % row-selector options) (ds :data)))
       (ds/add-or-update-column ds :count (map ds/row-count (ds :data))))
     (let [local-options {:keep-fn (get strategies strategy :first)}]
       (cond
         (sequential+? row-selector) (ds/unique-by identity (assoc local-options :column-name-seq row-selector) ds)
         (fn? row-selector) (ds/unique-by row-selector (if limit-columns
                                                         (assoc local-options :column-name-seq limit-columns)
                                                         local-options) ds)
         :else (ds/unique-by-column row-selector local-options ds))))))

;;;;;;;;;;;;
;; MISSING
;;;;;;;;;;;;

(defn select-or-drop-missing
  "Select rows with missing values"
  ([f ds] (select-or-drop-missing f ds nil))
  ([f ds cols-selector]
   (if (grouped? ds)
     (as-> ds ds
       (ds/add-or-update-column ds :data (map #(select-or-drop-missing f % cols-selector) (ds :data)))
       (ds/add-or-update-column ds :count (map ds/row-count (ds :data))))
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
(select-columns DS (comp #{:int64} :datatype))

(drop-columns DS :V1)
(drop-columns DS [:V1 :V2])
(drop-columns DS (comp #{:int64} :datatype))

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
