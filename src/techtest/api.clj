(ns techtest.api
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime.operations :as dtype-dt-ops]
            [tech.v2.datatype.readers.update :as update-rdr]
            [tech.v2.datatype.readers.concat :as concat-rdr]
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.ml.protocols.dataset :as prot]
            [clojure.string :as str]

            [tech.parallel.utils :as exporter]
            [techtest.api.utils :refer [map-kv map-v iterable-sequence?]]
            [techtest.api.group-by :refer [correct-group-count process-group-data]])
  (:refer-clojure :exclude [group-by drop concat])
  (:import [org.roaringbitmap RoaringBitmap]))

#_(set! *warn-on-reflection* true)

;; attempt to reorganized api

;; dataset

(exporter/export-symbols tech.ml.dataset
                         column-count
                         row-count
                         set-dataset-name
                         dataset-name
                         column
                         column-names
                         has-column?
                         write-csv!)

(exporter/export-symbols techtest.api.dataset
                         dataset?
                         dataset
                         shape
                         info
                         columns
                         rows)

(exporter/export-symbols techtest.api.group-by
                         group-by
                         ungroup
                         grouped?
                         unmark-group
                         as-regular-dataset
                         mark-as-group
                         groups->seq
                         groups->map)

;; concat

(defn concat
  [datasets]
  (apply ds/concat datasets))


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


;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DESCRIPTIVE FUNCTIONS
;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;
;; GROUPING
;;;;;;;;;;;;;


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
     (iterable-sequence? cols-selector) cols-selector
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
                          (clojure.core/concat col-or-seq (repeat (- cnt seq-cnt) nil))))
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
           (iterable-sequence? column-seq-or-gen)) (->> (ds/row-count ds)
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

(defn reorder-columns
  "Reorder columns using column selector(s). When column names are incomplete, the missing will be attached at the end."
  [ds cols-selector & cols-selectors]
  (let [selected-cols (->> cols-selectors
                           (map (partial select-column-names ds))
                           (reduce clojure.core/concat (select-column-names ds cols-selector)))
        rest-cols (select-column-names ds (complement (set selected-cols)))]
    (ds/select-columns ds (clojure.core/concat selected-cols rest-cols))))

(defn- try-convert-to-type
  [ds colname new-type]
  (ds/column-cast ds colname new-type))

(defn convert-column-type
  "Convert type of the column to the other type."
  ([ds coltype-map]
   (reduce (fn [ds [colname new-type]]
             (convert-column-type ds colname new-type)) ds coltype-map))
  ([ds colname new-type]
   (if (grouped? ds)
     (process-group-data ds #(convert-column-type % colname new-type))

     (if (iterable-sequence? new-type)
       (try-convert-to-type ds colname new-type)
       (if (= :object new-type)
         (try-convert-to-type ds colname [:object identity])
         (let [col (ds colname)]
           (condp = (dtype/get-datatype col)
             :string (ds/add-or-update-column ds colname (col/parse-column new-type col))
             :object (if (string? (dtype/get-value col 0))
                       (-> (try-convert-to-type ds colname :string)
                           (ds/column colname)
                           (->> (col/parse-column new-type)
                                (ds/add-or-update-column ds colname)))
                       (try-convert-to-type ds colname new-type))
             (try-convert-to-type ds colname new-type))))))))

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
     (iterable-sequence? rows-selector) (find-indexes-from-seq ds rows-selector)
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
    (iterable-sequence? agg-res) (->> agg-res
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
         (process-group-data #(aggregate % fn-map-or-seq options) true)
         (ungroup {:add-group-as-column true}))
     (cond
       (fn? fn-map-or-seq) (aggregate ds {:summary fn-map-or-seq})
       (iterable-sequence? fn-map-or-seq) (->> fn-map-or-seq
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
   (order-by ds cols-selector (if (iterable-sequence? cols-selector)
                                (repeat (count cols-selector) :asc)
                                [:asc])))
  ([ds cols-selector order-or-comparators]
   (if (grouped? ds)

     (process-group-data ds #(order-by % cols-selector order-or-comparators))

     (let [comp-fn (asc-desc-comparator order-or-comparators)]
       (cond
         (iterable-sequence? cols-selector) (ds/sort-by (apply juxt (map #(if (fn? %)
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

(defn- strategy-fold
  ([ds group-by-names] (strategy-fold ds group-by-names nil))
  ([ds group-by-names fold-fn]
   (let [target-names (select-column-names ds (complement (set group-by-names)))
         fold-fn (or fold-fn vec)]
     (-> (group-by ds group-by-names)
         (process-group-data (fn [ds]
                               (as-> ds ds
                                 (select-columns ds target-names)
                                 (dataset [(zipmap target-names (map fold-fn (ds/columns ds)))]))) true)
         (ungroup {:add-group-as-column true})))))

(defn unique-by
  ([ds] (unique-by ds (ds/column-names ds)))
  ([ds cols-selector] (unique-by ds cols-selector nil))
  ([ds cols-selector {:keys [strategy fold-fn limit-columns]
                      :or {strategy :first fold-fn vec}
                      :as options}]
   (if (grouped? ds)
     (process-group-data ds #(unique-by % cols-selector options) true)

     (if (= 1 (ds/row-count ds))
       ds
       (if (= strategy :fold)
         (strategy-fold ds (select-column-names ds cols-selector) fold-fn)
         (let [local-options {:keep-fn (get strategies strategy :first)}]
           (cond
             (iterable-sequence? cols-selector) (if (= (count cols-selector) 1)
                                                  (ds/unique-by-column (first cols-selector) local-options ds)
                                                  (ds/unique-by identity (assoc local-options :column-name-seq cols-selector) ds))
             (fn? cols-selector) (ds/unique-by cols-selector (if limit-columns
                                                               (assoc local-options :column-name-seq limit-columns)
                                                               local-options) ds)
             :else (ds/unique-by-column cols-selector local-options ds))))))))

;;;;;;;;;;;;
;; MISSING
;;;;;;;;;;;;

(defn- select-or-drop-missing
  "Select rows with missing values"
  ([f ds] (select-or-drop-missing f ds nil))
  ([f ds cols-selector]
   (if (grouped? ds)
     (process-group-data ds #(select-or-drop-missing f % cols-selector) true)

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

(defn- remove-from-rbitmap
  ^RoaringBitmap [^RoaringBitmap rb ks]
  (let [rb (.clone rb)]
    (reduce (fn [^RoaringBitmap rb ^long k]
              (.remove rb k)
              rb) rb ks)))

(defn- replace-missing-with-value
  [col missing value]
  (col/new-column (col/column-name col)
                  (update-rdr/update-reader col (cond
                                                  (map? value) value
                                                  (iterable-sequence? value) (zipmap missing (cycle value))
                                                  :else (bitmap/bitmap-value->bitmap-map missing value)))
                  {} (if (map? value)
                       (remove-from-rbitmap missing (keys value))
                       (RoaringBitmap.))))

(defn- missing-direction-prev
  ^long [^RoaringBitmap rb ^long idx]
  (.previousAbsentValue rb idx))

(defn- missing-direction-next
  ^long [^RoaringBitmap rb ^long idx]
  (.nextAbsentValue rb idx))

(defn- replace-missing-with-direction
  [f col missing value]
  (let [cnt (dtype/ecount col)
        step1 (replace-missing-with-value col missing (reduce (fn [m v]
                                                                (let [vv (f missing v)]
                                                                  (if (< -1 vv cnt)
                                                                    (assoc m v (dtype/get-value col vv))
                                                                    m))) {} missing))]
    (if (or (nil? value)
            (empty? (col/missing step1)))
      step1
      (replace-missing-with-value step1 (col/missing step1) value))))

(defn- replace-missing-with-strategy
  [col missing value strategy]
  (let [value (if (fn? value)
                (value (dtype/->reader col (dtype/get-datatype col) {:missing-policy :elide}))
                value)]
    (condp = strategy
      :down (replace-missing-with-direction missing-direction-prev col missing value)
      :up (replace-missing-with-direction missing-direction-next col missing value)
      (replace-missing-with-value col missing value))))

(defn replace-missing
  ([ds cols-selector value] (replace-missing ds cols-selector value nil))
  ([ds cols-selector value {:keys [strategy]
                            :or {strategy :value}
                            :as options}]

   (if (grouped? ds)

     (process-group-data ds #(replace-missing % cols-selector value options))
     
     (let [cols (select-column-names ds cols-selector)]
       (reduce (fn [ds colname]
                 (let [col (ds colname)
                       missing (col/missing col)]
                   (if-not (empty? missing)
                     (ds/add-or-update-column ds (replace-missing-with-strategy col missing value strategy))
                     ds))) ds cols)))))

;;;;;;;;;;;;;;;
;; JOIN/SPLIT
;;;;;;;;;;;;;;;

(defn join-columns
  ([ds target-column cols-selector] (join-columns ds target-column cols-selector nil))
  ([ds target-column cols-selector {:keys [separator missing-subst drop-columns? result-type]
                                    :or {separator "-" drop-columns? true result-type :string}
                                    :as options}]
   (if (grouped? ds)
     
     (process-group-data ds #(join-columns % target-column cols-selector options))

     (let [cols (select-columns ds cols-selector)
           missing-subst-fn #(map (fn [row] (or row missing-subst)) %)
           join-function (condp = result-type
                           :map (let [col-names (ds/column-names cols)]
                                  #(zipmap col-names %))
                           :seq seq
                           (let [sep (if (iterable-sequence? separator)
                                       (cycle separator)
                                       (repeat separator))]
                             (fn [row] (->> row
                                           (interleave sep)
                                           (rest)
                                           (apply str)))))]
       
       (let [result (ds/add-or-update-column ds target-column (->> (ds/value-reader cols options)
                                                                   (map (comp join-function missing-subst-fn))))]

         (if drop-columns? (drop-columns result cols-selector) result))))))

(defn- separate-column->columns
  [col target-columns replace-missing separator-fn]
  (let [res (pmap separator-fn col)]
    (pmap (fn [idx colname]
            (col/new-column colname (map #(replace-missing (nth % idx)) res))) (range) target-columns)))

(defn- prepare-missing-subst-fn
  [missing-subst]
  (let [missing-subst-fn (if (or (set? missing-subst)
                                 (fn? missing-subst))
                           missing-subst
                           (partial = missing-subst))]
    (fn [res]
      (map #(if (missing-subst-fn %) nil %) res))))

(defn separate-column
  ([ds column target-columns] (separate-column ds column target-columns identity))
  ([ds column target-columns separator] (separate-column ds column target-columns separator nil))
  ([ds column target-columns separator {:keys [missing-subst drop-column?]
                                        :or {drop-column? true}
                                        :as options}]
   (if (grouped? ds)
     
     (process-group-data ds #(separate-column % column target-columns separator options))

     (let [separator-fn (cond
                          (string? separator) (let [pat (re-pattern separator)]
                                                #(str/split (str %) pat))
                          (instance? java.util.regex.Pattern separator) #(rest (re-matches separator (str %)))
                          :else separator)
           replace-missing (if missing-subst
                             (prepare-missing-subst-fn missing-subst)
                             identity)
           result (separate-column->columns (ds column) target-columns replace-missing separator-fn)
           [dataset-before dataset-after] (map (partial ds/select-columns ds)
                                               (split-with #(not= % column)
                                                           (ds/column-names ds)))]
       (-> (if drop-column?
             dataset-before
             (ds/add-column dataset-before (ds column)))
           (ds/append-columns result)
           (ds/append-columns (ds/columns (ds/drop-columns dataset-after [column]))))))))

;;;;;;;;;;;;
;; RESHAPE
;;;;;;;;;;;;

(defn- regroup-cols-from-template
  [ds cols target-cols value-name column-split-fn]
  (let [template? (some nil? target-cols)
        pre-groups (->> cols
                        (map (fn [col-name]
                               (let [col (ds col-name)
                                     split (column-split-fn col-name)
                                     buff (if template? {} {value-name col})]
                                 (into buff (mapv (fn [k v] (if k [k v] [v col]))
                                                  target-cols split))))))
        groups (-> (->> target-cols
                        (remove nil?)
                        (map #(fn [n] (get n %)))
                        (apply juxt))
                   (clojure.core/group-by pre-groups)
                   (vals))]
    (map #(reduce merge %) groups)))

(defn- cols->pre-longer
  ([ds cols names value-name column-splitter]
   (let [column-split-fn (cond (instance? java.util.regex.Pattern column-splitter) (comp rest #(re-find column-splitter (str %)))
                               (fn? column-splitter) column-splitter
                               :else vector)]
     (regroup-cols-from-template ds cols names value-name column-split-fn))))

(defn- pre-longer->target-cols
  [ds cnt m]
  (let [new-cols (map (fn [[col-name maybe-column]]
                        (if (col/is-column? maybe-column)
                          (col/set-name maybe-column col-name)
                          (col/new-column col-name (dtype/const-reader maybe-column cnt {:datatype :object})))) m)]
    (ds/append-columns ds new-cols)))

(defn pivot->longer
  "`tidyr` pivot_longer api"
  ([ds cols-selector] (pivot->longer ds cols-selector nil))
  ([ds cols-selector {:keys [target-cols value-column-name splitter drop-missing? meta-field datatypes]
                      :or {target-cols :$column
                           value-column-name :$value
                           drop-missing? true}}]
   (let [cols (select-column-names ds cols-selector meta-field)
         target-cols (if (iterable-sequence? target-cols) target-cols [target-cols])
         groups (cols->pre-longer ds cols target-cols value-column-name splitter)
         cols-to-add (keys (first groups))
         ds-template (drop-columns ds cols)
         cnt (ds/row-count ds-template)]
     (as-> (ds/set-metadata (->> groups                                        
                                 (map (partial pre-longer->target-cols ds-template cnt))
                                 (apply ds/concat))
                            (ds/metadata ds)) final-ds
       (if drop-missing? (drop-missing final-ds cols-to-add) final-ds)
       (if datatypes (convert-column-type final-ds datatypes) final-ds)
       (reorder-columns final-ds (ds/column-names ds-template) (remove nil? target-cols))))))

(defn- drop-join-leftovers
  [data join-name]
  (drop-columns data (-> (meta data)
                         :right-column-names
                         (get join-name))))

(defn- perform-join
  [join-ds curr-ds join-name]
  (ds/left-join join-name curr-ds join-ds))

(defn- make-apply-join-fn
  "Perform left-join on groups and create new columns"
  [group-name->names single-value? value-names join-name starting-ds-count fold-fn rename-map]
  (fn [curr-ds {:keys [name group-id count data]}]
    (let [col-name (str/join "_" (->> name
                                      group-name->names
                                      (remove nil?))) ;; source names
          col-name (get rename-map col-name col-name)
          target-names (if single-value?
                         [col-name]
                         (map #(str % "-" col-name) value-names)) ;; traget column names
          rename-map (zipmap value-names target-names) ;; renaming map
          data (-> data
                   (rename-columns rename-map) ;; rename value column
                   (select-columns (conj target-names join-name)) ;; select rhs for join
                   (perform-join curr-ds join-name) ;; perform left join
                   (drop-join-leftovers join-name))] ;; drop unnecessary leftovers
      (if (> (ds/row-count data) starting-ds-count) ;; in case when there were multiple values, create vectors
        (if fold-fn
          (strategy-fold data (select-column-names data (complement (set target-names))) fold-fn)
          (do (println "WARNING: multiple values in result detected, data should be rolled in.")
              data))
        data))))

(defn pivot->wider
  ([ds cols-selector value-columns] (pivot->wider ds cols-selector value-columns nil))
  ([ds cols-selector value-columns {:keys [fold-fn rename-map]}]
   (let [col-names (select-column-names ds cols-selector) ;; columns to be unrolled
         value-names (select-column-names ds value-columns) ;; columns to be used as values
         single-value? (= (count value-names) 1) ;; maybe this is one column? (different name creation rely on this)
         rest-cols (->> (clojure.core/concat col-names value-names)
                        (set)
                        (complement)
                        (select-column-names ds)) ;; the columns used in join
         join-on-single? (= (count rest-cols) 1) ;; mayve this is one column? (different join column creation)
         join-name (if join-on-single?
                     (first rest-cols)
                     (gensym (apply str "^____" rest-cols))) ;; generate join column name
         ;; col-to-drop (col-to-drop-name join-name) ;; what to drop after join
         pre-ds (if join-on-single?
                  ds
                  (join-columns ds join-name rest-cols {:result-type :seq
                                                        :drop-columns? true})) ;; t.m.ds doesn't have join on multiple columns, so we need to create single column to be used fo join
         starting-ds (unique-by (select-columns pre-ds join-name)) ;; left join source dataset
         starting-ds-count (ds/row-count starting-ds) ;; how much records to expect
         grouped-ds (group-by pre-ds col-names) ;; group by columns which values will create new columns
         group-name->names (->> col-names
                                (map #(fn [m] (get m %)))
                                (apply juxt)) ;; create function which extract new column name
         result (reduce (make-apply-join-fn group-name->names single-value? value-names
                                            join-name starting-ds-count fold-fn rename-map)
                        starting-ds
                        (ds/mapseq-reader grouped-ds))] ;; perform join on groups and create new columns
     (-> (if join-on-single? ;; finalize, recreate original columns from join column, and reorder stuff
           result
           (-> (separate-column result join-name rest-cols identity {:drop-column? true})
               (reorder-columns rest-cols)))
         (ds/set-dataset-name (ds/dataset-name ds))))))



;;;;;;;;;;;;;;
;; USE CASES
;;;;;;;;;;;;;;


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

(def DSm2 (dataset {:a [nil nil nil 1 2 nil 3 4 nil nil nil 11 nil]
                    :b [nil 2   2   2 2 3   nil 3 nil   3   nil   4  nil]}))


(ungroup (group-by DS :V1) {:dataset-name "ungrouped"})
(ungroup (group-by DS [:V1 :V3]) {:add-group-id-as-column true})
(ungroup (group-by DS (juxt :V1 :V4)) {:add-group-as-column "my group"})
(ungroup (group-by DS #(< (:V2 %) 4)) {:add-group-as-column true})
(ungroup (group-by DS (comp #{\B \C} :V4)) {:add-group-as-column true})


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
(unique-by DS :V1 {:strategy :fold})

(unique-by DS :V4 {:strategy :fold
                   :fold-fn dfn/sum})

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

(replace-missing DSm2 :a 222)
(replace-missing DSm2 [:a :b] 222)

(replace-missing DSm2 :a nil {:strategy :up})
(replace-missing DSm2 :a nil {:strategy :down})
(replace-missing DSm2 :a 999 {:strategy :up})
(replace-missing DSm2 :a 999 {:strategy :down})
(replace-missing DSm2 [:a :b] 999 {:strategy :down})

(replace-missing DSm2 [:a :b] dfn/median)

(ungroup (replace-missing (group-by DSm2 :b) [:a] 11 {:strategy :up}))

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
                                                  (mod v 1.0)]) {:drop-column? false})

(separate-column DS :V3 [:int-part :frac-part] (fn [^double v]
                                                 [(int (quot v 1.0))
                                                  (mod v 1.0)]) {:missing-subst #{0 0.0}})

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
    (rename-columns {:$group-name-0 :symbol
                     :$group-name-1 :year})
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
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; RESHAPE TESTS

;; TESTS

(def relig-income (dataset "data/relig_income.csv"))
(pivot->longer relig-income (complement #{"religion"}))

;; => data/relig_income.csv [180 3]:
;;    |                religion |           :$column | :$value |
;;    |-------------------------+--------------------+---------|
;;    |                Agnostic |              <$10k |      27 |
;;    |                 Atheist |              <$10k |      12 |
;;    |                Buddhist |              <$10k |      27 |
;;    |                Catholic |              <$10k |     418 |
;;    |      Don’t know/refused |              <$10k |      15 |
;;    |        Evangelical Prot |              <$10k |     575 |
;;    |                   Hindu |              <$10k |       1 |
;;    | Historically Black Prot |              <$10k |     228 |
;;    |       Jehovah's Witness |              <$10k |      20 |
;;    |                  Jewish |              <$10k |      19 |
;;    |           Mainline Prot |              <$10k |     289 |
;;    |                  Mormon |              <$10k |      29 |
;;    |                  Muslim |              <$10k |       6 |
;;    |                Orthodox |              <$10k |      13 |
;;    |         Other Christian |              <$10k |       9 |
;;    |            Other Faiths |              <$10k |      20 |
;;    |   Other World Religions |              <$10k |       5 |
;;    |            Unaffiliated |              <$10k |     217 |
;;    |                Agnostic | Don't know/refused |      96 |
;;    |                 Atheist | Don't know/refused |      76 |
;;    |                Buddhist | Don't know/refused |      54 |
;;    |                Catholic | Don't know/refused |    1489 |
;;    |      Don’t know/refused | Don't know/refused |     116 |
;;    |        Evangelical Prot | Don't know/refused |    1529 |
;;    |                   Hindu | Don't know/refused |      37 |

(def bilboard (-> (dataset "data/billboard.csv.gz")
                  (drop-columns #(= :boolean %) {:meta-field :datatype}))) ;; drop some boolean columns, tidyr just skips them

(pivot->longer bilboard #(str/starts-with? % "wk") {:target-cols :week
                                                    :value-column-name :rank})

;; => data/billboard.csv.gz [5307 5]:
;;    |              artist |                   track | date.entered | :week | :rank |
;;    |---------------------+-------------------------+--------------+-------+-------|
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |  wk35 |     4 |
;;    |       Braxton, Toni |    He Wasn't Man Enough |   2000-03-18 |  wk35 |    34 |
;;    |               Creed |                  Higher |   1999-09-11 |  wk35 |    22 |
;;    |               Creed |     With Arms Wide Open |   2000-05-13 |  wk35 |     5 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |  wk35 |     8 |
;;    |                 Joe |            I Wanna Know |   2000-01-01 |  wk35 |     5 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |  wk35 |    14 |
;;    |    Vertical Horizon |     Everything You Want |   2000-01-22 |  wk35 |    27 |
;;    |     matchbox twenty |                    Bent |   2000-04-29 |  wk35 |    33 |
;;    |               Creed |                  Higher |   1999-09-11 |  wk55 |    21 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |  wk55 |    22 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |  wk19 |    18 |
;;    |        3 Doors Down |                   Loser |   2000-10-21 |  wk19 |    73 |
;;    |                98^0 | Give Me Just One Nig... |   2000-08-19 |  wk19 |    93 |
;;    |             Aaliyah |           I Don't Wanna |   2000-01-29 |  wk19 |    83 |
;;    |             Aaliyah |               Try Again |   2000-03-18 |  wk19 |     3 |
;;    |      Adams, Yolanda |           Open My Heart |   2000-08-26 |  wk19 |    79 |
;;    | Aguilera, Christina | Come On Over Baby (A... |   2000-08-05 |  wk19 |    23 |
;;    | Aguilera, Christina |           I Turn To You |   2000-04-15 |  wk19 |    29 |
;;    | Aguilera, Christina |       What A Girl Wants |   1999-11-27 |  wk19 |    18 |
;;    |        Alice Deejay |        Better Off Alone |   2000-04-08 |  wk19 |    79 |
;;    |               Amber |                  Sexual |   1999-07-17 |  wk19 |    95 |
;;    |       Anthony, Marc |             My Baby You |   2000-09-16 |  wk19 |    91 |
;;    |       Anthony, Marc |          You Sang To Me |   2000-02-26 |  wk19 |     9 |
;;    |               Avant |           My First Love |   2000-11-04 |  wk19 |    81 |

(pivot->longer bilboard #(str/starts-with? % "wk") {:target-cols :week
                                                    :value-column-name :rank
                                                    :splitter #"wk(.*)"
                                                    :datatypes {:week :int16}})

;; => data/billboard.csv.gz [5307 5]:
;;    |              artist |                   track | date.entered | :week | :rank |
;;    |---------------------+-------------------------+--------------+-------+-------|
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    46 |    21 |
;;    |               Creed |                  Higher |   1999-09-11 |    46 |     7 |
;;    |               Creed |     With Arms Wide Open |   2000-05-13 |    46 |    37 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |    46 |    31 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |    46 |     5 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |    51 |    42 |
;;    |               Creed |                  Higher |   1999-09-11 |    51 |    14 |
;;    |         Hill, Faith |                 Breathe |   1999-11-06 |    51 |    49 |
;;    |            Lonestar |                  Amazed |   1999-06-05 |    51 |    12 |
;;    |               2 Pac | Baby Don't Cry (Keep... |   2000-02-26 |     6 |    94 |
;;    |        3 Doors Down |              Kryptonite |   2000-04-08 |     6 |    57 |
;;    |        3 Doors Down |                   Loser |   2000-10-21 |     6 |    65 |
;;    |            504 Boyz |           Wobble Wobble |   2000-04-15 |     6 |    31 |
;;    |                98^0 | Give Me Just One Nig... |   2000-08-19 |     6 |    19 |
;;    |             Aaliyah |           I Don't Wanna |   2000-01-29 |     6 |    35 |
;;    |             Aaliyah |               Try Again |   2000-03-18 |     6 |    18 |
;;    |      Adams, Yolanda |           Open My Heart |   2000-08-26 |     6 |    67 |
;;    |       Adkins, Trace |                    More |   2000-04-29 |     6 |    69 |
;;    | Aguilera, Christina | Come On Over Baby (A... |   2000-08-05 |     6 |    18 |
;;    | Aguilera, Christina |           I Turn To You |   2000-04-15 |     6 |    19 |
;;    | Aguilera, Christina |       What A Girl Wants |   1999-11-27 |     6 |    13 |
;;    |        Alice Deejay |        Better Off Alone |   2000-04-08 |     6 |    36 |
;;    |               Amber |                  Sexual |   1999-07-17 |     6 |    93 |
;;    |       Anthony, Marc |             My Baby You |   2000-09-16 |     6 |    81 |
;;    |       Anthony, Marc |          You Sang To Me |   2000-02-26 |     6 |    27 |

(def who (dataset "data/who.csv.gz"))

(pivot->longer who #(str/starts-with? % "new") {:target-cols [:diagnosis :gender :age]
                                                :splitter #"new_?(.*)_(.)(.*)"
                                                :value-column-name :count})

;; => data/who.csv.gz [76046 8]:
;;    |                           country | iso2 | iso3 | year | :diagnosis | :gender | :age | :count |
;;    |-----------------------------------+------+------+------+------------+---------+------+--------|
;;    |                           Albania |   AL |  ALB | 2013 |        rel |       m | 1524 |     60 |
;;    |                           Algeria |   DZ |  DZA | 2013 |        rel |       m | 1524 |   1021 |
;;    |                           Andorra |   AD |  AND | 2013 |        rel |       m | 1524 |      0 |
;;    |                            Angola |   AO |  AGO | 2013 |        rel |       m | 1524 |   2992 |
;;    |                          Anguilla |   AI |  AIA | 2013 |        rel |       m | 1524 |      0 |
;;    |               Antigua and Barbuda |   AG |  ATG | 2013 |        rel |       m | 1524 |      1 |
;;    |                         Argentina |   AR |  ARG | 2013 |        rel |       m | 1524 |   1124 |
;;    |                           Armenia |   AM |  ARM | 2013 |        rel |       m | 1524 |    116 |
;;    |                         Australia |   AU |  AUS | 2013 |        rel |       m | 1524 |    105 |
;;    |                           Austria |   AT |  AUT | 2013 |        rel |       m | 1524 |     44 |
;;    |                        Azerbaijan |   AZ |  AZE | 2013 |        rel |       m | 1524 |    958 |
;;    |                           Bahamas |   BS |  BHS | 2013 |        rel |       m | 1524 |      2 |
;;    |                           Bahrain |   BH |  BHR | 2013 |        rel |       m | 1524 |     13 |
;;    |                        Bangladesh |   BD |  BGD | 2013 |        rel |       m | 1524 |  14705 |
;;    |                          Barbados |   BB |  BRB | 2013 |        rel |       m | 1524 |      0 |
;;    |                           Belarus |   BY |  BLR | 2013 |        rel |       m | 1524 |    162 |
;;    |                           Belgium |   BE |  BEL | 2013 |        rel |       m | 1524 |     63 |
;;    |                            Belize |   BZ |  BLZ | 2013 |        rel |       m | 1524 |      8 |
;;    |                             Benin |   BJ |  BEN | 2013 |        rel |       m | 1524 |    301 |
;;    |                           Bermuda |   BM |  BMU | 2013 |        rel |       m | 1524 |      0 |
;;    |                            Bhutan |   BT |  BTN | 2013 |        rel |       m | 1524 |    180 |
;;    |  Bolivia (Plurinational State of) |   BO |  BOL | 2013 |        rel |       m | 1524 |   1470 |
;;    | Bonaire, Saint Eustatius and Saba |   BQ |  BES | 2013 |        rel |       m | 1524 |      0 |
;;    |            Bosnia and Herzegovina |   BA |  BIH | 2013 |        rel |       m | 1524 |     57 |
;;    |                          Botswana |   BW |  BWA | 2013 |        rel |       m | 1524 |    423 |

(def family (dataset "data/family.csv"))

(pivot->longer family (complement #{"family"}) {:target-cols [nil :child]
                                                :splitter #(str/split % #"_")
                                                :datatypes {"gender" :int16}})

;; => data/family.csv [9 4]:
;;    | family | :child |        dob | gender |
;;    |--------+--------+------------+--------|
;;    |      1 | child1 | 1998-11-26 |      1 |
;;    |      2 | child1 | 1996-06-22 |      2 |
;;    |      3 | child1 | 2002-07-11 |      2 |
;;    |      4 | child1 | 2004-10-10 |      1 |
;;    |      5 | child1 | 2000-12-05 |      2 |
;;    |      1 | child2 | 2000-01-29 |      2 |
;;    |      3 | child2 | 2004-04-05 |      2 |
;;    |      4 | child2 | 2009-08-27 |      1 |
;;    |      5 | child2 | 2005-02-28 |      1 |

(def anscombe (dataset "data/anscombe.csv"))

(pivot->longer anscombe :all {:splitter #"(.)(.)"
                              :target-cols [nil :set]})

;; => data/anscombe.csv [44 3]:
;;    | :set |  x |     y |
;;    |------+----+-------|
;;    |    4 | 10 | 8.040 |
;;    |    4 |  8 | 6.950 |
;;    |    4 | 13 | 7.580 |
;;    |    4 |  9 | 8.810 |
;;    |    4 | 11 | 8.330 |
;;    |    4 | 14 | 9.960 |
;;    |    4 |  6 | 7.240 |
;;    |    4 |  4 | 4.260 |
;;    |    4 | 12 | 10.84 |
;;    |    4 |  7 | 4.820 |
;;    |    4 |  5 | 5.680 |
;;    |    4 | 10 | 9.140 |
;;    |    4 |  8 | 8.140 |
;;    |    4 | 13 | 8.740 |
;;    |    4 |  9 | 8.770 |
;;    |    4 | 11 | 9.260 |
;;    |    4 | 14 | 8.100 |
;;    |    4 |  6 | 6.130 |
;;    |    4 |  4 | 3.100 |
;;    |    4 | 12 | 9.130 |
;;    |    4 |  7 | 7.260 |
;;    |    4 |  5 | 4.740 |
;;    |    4 | 10 | 7.460 |
;;    |    4 |  8 | 6.770 |
;;    |    4 | 13 | 12.74 |

(def pnl (dataset {:x [1 2 3 4]
                   :a [1 1 0 0]
                   :b [0 1 1 1]
                   :y1 (repeatedly 4 rand)
                   :y2 (repeatedly 4 rand)
                   :z1 [3 3 3 3]
                   :z2 [-2 -2 -2 -2]}))

(pivot->longer pnl [:y1 :y2 :z1 :z2] {:target-cols [nil :times]
                                      :splitter #":(.)(.)"})

;; => _unnamed [8 6]:
;;    | :x | :a | :b | :times |       y |  z |
;;    |----+----+----+--------+---------+----|
;;    |  1 |  1 |  0 |      1 |  0.7929 |  3 |
;;    |  2 |  1 |  1 |      1 |  0.4401 |  3 |
;;    |  3 |  0 |  1 |      1 |  0.6825 |  3 |
;;    |  4 |  0 |  1 |      1 |  0.2481 |  3 |
;;    |  1 |  1 |  0 |      2 | 0.09633 | -2 |
;;    |  2 |  1 |  1 |      2 |  0.8609 | -2 |
;;    |  3 |  0 |  1 |      2 |  0.6604 | -2 |
;;    |  4 |  0 |  1 |      2 |  0.7120 | -2 |

;; wider

(def fish (dataset "data/fish_encounters.csv"))

(pivot->wider fish "station" "seen")
;; => data/fish_encounters.csv [19 12]:
;;    | fish | Rstr | Base_TD | I80_1 | Release | MAE | BCE2 | MAW | BCW2 | BCE | Lisbon | BCW |
;;    |------+------+---------+-------+---------+-----+------+-----+------+-----+--------+-----|
;;    | 4842 |    1 |       1 |     1 |       1 |   1 |    1 |   1 |    1 |   1 |      1 |   1 |
;;    | 4843 |    1 |       1 |     1 |       1 |   1 |    1 |   1 |    1 |   1 |      1 |   1 |
;;    | 4844 |    1 |       1 |     1 |       1 |   1 |    1 |   1 |    1 |   1 |      1 |   1 |
;;    | 4850 |    1 |       1 |     1 |       1 |     |      |     |      |   1 |        |   1 |
;;    | 4857 |    1 |       1 |     1 |       1 |     |    1 |     |    1 |   1 |      1 |   1 |
;;    | 4858 |    1 |       1 |     1 |       1 |   1 |    1 |   1 |    1 |   1 |      1 |   1 |
;;    | 4861 |    1 |       1 |     1 |       1 |   1 |    1 |   1 |    1 |   1 |      1 |   1 |
;;    | 4862 |    1 |       1 |     1 |       1 |     |    1 |     |    1 |   1 |      1 |   1 |
;;    | 4864 |      |         |     1 |       1 |     |      |     |      |     |        |     |
;;    | 4865 |      |         |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4845 |    1 |       1 |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4847 |      |         |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4848 |    1 |         |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4849 |      |         |     1 |       1 |     |      |     |      |     |        |     |
;;    | 4851 |      |         |     1 |       1 |     |      |     |      |     |        |     |
;;    | 4854 |      |         |     1 |       1 |     |      |     |      |     |        |     |
;;    | 4855 |    1 |       1 |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4859 |    1 |       1 |     1 |       1 |     |      |     |      |     |      1 |     |
;;    | 4863 |      |         |     1 |       1 |     |      |     |      |     |        |     |

(def warpbreaks (dataset "data/warpbreaks.csv"))

(-> warpbreaks
    (group-by ["wool" "tension"])
    (aggregate {:n ds/row-count}))

;; => null [6 3]:
;;    | wool | tension | :n |
;;    |------+---------+----|
;;    |    A |       H |  9 |
;;    |    B |       H |  9 |
;;    |    A |       L |  9 |
;;    |    A |       M |  9 |
;;    |    B |       L |  9 |
;;    |    B |       M |  9 |

(pivot->wider (select-columns warpbreaks ["wool" "tension" "breaks"]) "wool" "breaks" {:fold-fn vec})

;; => data/warpbreaks.csv [3 3]:
;;    | tension |                            B |                            A |
;;    |---------+------------------------------+------------------------------|
;;    |       M | [42 26 19 16 39 28 21 39 29] | [18 21 29 17 12 18 35 30 36] |
;;    |       H | [20 21 24 17 13 15 15 16 28] | [36 21 24 18 10 43 28 15 26] |
;;    |       L | [27 14 29 19 29 31 41 20 44] | [26 30 54 25 70 52 51 26 67] |

(pivot->wider (select-columns warpbreaks ["wool" "tension" "breaks"]) "wool" "breaks" {:fold-fn dfn/mean})

;; => data/warpbreaks.csv [3 3]:
;;    | tension |     B |     A |
;;    |---------+-------+-------|
;;    |       H | 18.78 | 24.56 |
;;    |       M | 28.78 | 24.00 |
;;    |       L | 28.22 | 44.56 |

(def production (dataset "data/production.csv"))

(pivot->wider production ["product" "country"] "production")

;; => data/production.csv [15 4]:
;;    | year |     A_AI |    B_EI |     B_AI |
;;    |------+----------+---------+----------|
;;    | 2000 |    1.637 |   1.405 | -0.02618 |
;;    | 2001 |   0.1587 | -0.5962 |  -0.6886 |
;;    | 2002 |   -1.568 | -0.2657 |  0.06249 |
;;    | 2003 |  -0.4446 |  0.6526 |  -0.7234 |
;;    | 2004 | -0.07134 |  0.6256 |   0.4725 |
;;    | 2005 |    1.612 |  -1.345 |  -0.9417 |
;;    | 2006 |  -0.7043 | -0.9718 |  -0.3478 |
;;    | 2007 |   -1.536 |  -1.697 |   0.5243 |
;;    | 2008 |   0.8391 | 0.04556 |    1.832 |
;;    | 2009 |  -0.3742 |   1.193 |   0.1071 |
;;    | 2010 |  -0.7116 |  -1.606 |  -0.3290 |
;;    | 2011 |    1.128 | -0.7724 |   -1.783 |
;;    | 2012 |    1.457 |  -2.503 |   0.6113 |
;;    | 2013 |   -1.559 |  -1.628 |  -0.7853 |
;;    | 2014 |  -0.1170 | 0.03330 |   0.9784 |

(def income (dataset "data/us_rent_income.csv"))

(pivot->wider income "variable" ["estimate" "moe"])

;; => data/us_rent_income.csv [52 6]:
;;    | GEOID |                 NAME | estimate-rent | moe-rent | estimate-income | moe-income |
;;    |-------+----------------------+---------------+----------+-----------------+------------|
;;    |     1 |              Alabama |           747 |        3 |           24476 |        136 |
;;    |     2 |               Alaska |          1200 |       13 |           32940 |        508 |
;;    |     4 |              Arizona |           972 |        4 |           27517 |        148 |
;;    |     5 |             Arkansas |           709 |        5 |           23789 |        165 |
;;    |     6 |           California |          1358 |        3 |           29454 |        109 |
;;    |     8 |             Colorado |          1125 |        5 |           32401 |        109 |
;;    |     9 |          Connecticut |          1123 |        5 |           35326 |        195 |
;;    |    10 |             Delaware |          1076 |       10 |           31560 |        247 |
;;    |    11 | District of Columbia |          1424 |       17 |           43198 |        681 |
;;    |    12 |              Florida |          1077 |        3 |           25952 |         70 |
;;    |    13 |              Georgia |           927 |        3 |           27024 |        106 |
;;    |    15 |               Hawaii |          1507 |       18 |           32453 |        218 |
;;    |    16 |                Idaho |           792 |        7 |           25298 |        208 |
;;    |    17 |             Illinois |           952 |        3 |           30684 |         83 |
;;    |    18 |              Indiana |           782 |        3 |           27247 |        117 |
;;    |    19 |                 Iowa |           740 |        4 |           30002 |        143 |
;;    |    20 |               Kansas |           801 |        5 |           29126 |        208 |
;;    |    21 |             Kentucky |           713 |        4 |           24702 |        159 |
;;    |    22 |            Louisiana |           825 |        4 |           25086 |        155 |
;;    |    23 |                Maine |           808 |        7 |           26841 |        187 |
;;    |    24 |             Maryland |          1311 |        5 |           37147 |        152 |
;;    |    25 |        Massachusetts |          1173 |        5 |           34498 |        199 |
;;    |    26 |             Michigan |           824 |        3 |           26987 |         82 |
;;    |    27 |            Minnesota |           906 |        4 |           32734 |        189 |
;;    |    28 |          Mississippi |           740 |        5 |           22766 |        194 |

(def contacts (dataset "data/contacts.csv"))

(pivot->wider contacts "field" "value")

;; => data/contacts.csv [3 4]:
;;    | person_id |           email |             name | company |
;;    |-----------+-----------------+------------------+---------|
;;    |         1 |                 |   Jiena McLellan |  Toyota |
;;    |         2 | john@google.com |       John Smith |  google |
;;    |         3 |                 | Huxley Ratcliffe |         |

(def world-bank-pop (dataset "data/world_bank_pop.csv.gz"))

(def pop2 (pivot->longer world-bank-pop (map str (range 2000 2018)) {:drop-missing? false
                                                                     :target-cols ["year"]
                                                                     :value-column-name "value"}))
pop2

;; => data/world_bank_pop.csv.gz [19008 4]:
;;    | country |   indicator | year |     value |
;;    |---------+-------------+------+-----------|
;;    |     ABW | SP.URB.TOTL | 2013 | 4.436E+04 |
;;    |     ABW | SP.URB.GROW | 2013 |    0.6695 |
;;    |     ABW | SP.POP.TOTL | 2013 | 1.032E+05 |
;;    |     ABW | SP.POP.GROW | 2013 |    0.5929 |
;;    |     AFG | SP.URB.TOTL | 2013 | 7.734E+06 |
;;    |     AFG | SP.URB.GROW | 2013 |     4.193 |
;;    |     AFG | SP.POP.TOTL | 2013 | 3.173E+07 |
;;    |     AFG | SP.POP.GROW | 2013 |     3.315 |
;;    |     AGO | SP.URB.TOTL | 2013 | 1.612E+07 |
;;    |     AGO | SP.URB.GROW | 2013 |     4.723 |
;;    |     AGO | SP.POP.TOTL | 2013 | 2.600E+07 |
;;    |     AGO | SP.POP.GROW | 2013 |     3.532 |
;;    |     ALB | SP.URB.TOTL | 2013 | 1.604E+06 |
;;    |     ALB | SP.URB.GROW | 2013 |     1.744 |
;;    |     ALB | SP.POP.TOTL | 2013 | 2.895E+06 |
;;    |     ALB | SP.POP.GROW | 2013 |   -0.1832 |
;;    |     AND | SP.URB.TOTL | 2013 | 7.153E+04 |
;;    |     AND | SP.URB.GROW | 2013 |    -2.119 |
;;    |     AND | SP.POP.TOTL | 2013 | 8.079E+04 |
;;    |     AND | SP.POP.GROW | 2013 |    -2.013 |
;;    |     ARB | SP.URB.TOTL | 2013 | 2.186E+08 |
;;    |     ARB | SP.URB.GROW | 2013 |     2.783 |
;;    |     ARB | SP.POP.TOTL | 2013 | 3.817E+08 |
;;    |     ARB | SP.POP.GROW | 2013 |     2.249 |
;;    |     ARE | SP.URB.TOTL | 2013 | 7.661E+06 |

(def pop3 (-> (separate-column pop2 "indicator" ["area" "variable"] #(rest (str/split % #"\.")))))

pop3

;; => data/world_bank_pop.csv.gz [19008 5]:
;;    | country | area | variable | year |     value |
;;    |---------+------+----------+------+-----------|
;;    |     ABW |  URB |     TOTL | 2013 | 4.436E+04 |
;;    |     ABW |  URB |     GROW | 2013 |    0.6695 |
;;    |     ABW |  POP |     TOTL | 2013 | 1.032E+05 |
;;    |     ABW |  POP |     GROW | 2013 |    0.5929 |
;;    |     AFG |  URB |     TOTL | 2013 | 7.734E+06 |
;;    |     AFG |  URB |     GROW | 2013 |     4.193 |
;;    |     AFG |  POP |     TOTL | 2013 | 3.173E+07 |
;;    |     AFG |  POP |     GROW | 2013 |     3.315 |
;;    |     AGO |  URB |     TOTL | 2013 | 1.612E+07 |
;;    |     AGO |  URB |     GROW | 2013 |     4.723 |
;;    |     AGO |  POP |     TOTL | 2013 | 2.600E+07 |
;;    |     AGO |  POP |     GROW | 2013 |     3.532 |
;;    |     ALB |  URB |     TOTL | 2013 | 1.604E+06 |
;;    |     ALB |  URB |     GROW | 2013 |     1.744 |
;;    |     ALB |  POP |     TOTL | 2013 | 2.895E+06 |
;;    |     ALB |  POP |     GROW | 2013 |   -0.1832 |
;;    |     AND |  URB |     TOTL | 2013 | 7.153E+04 |
;;    |     AND |  URB |     GROW | 2013 |    -2.119 |
;;    |     AND |  POP |     TOTL | 2013 | 8.079E+04 |
;;    |     AND |  POP |     GROW | 2013 |    -2.013 |
;;    |     ARB |  URB |     TOTL | 2013 | 2.186E+08 |
;;    |     ARB |  URB |     GROW | 2013 |     2.783 |
;;    |     ARB |  POP |     TOTL | 2013 | 3.817E+08 |
;;    |     ARB |  POP |     GROW | 2013 |     2.249 |
;;    |     ARE |  URB |     TOTL | 2013 | 7.661E+06 |

(pivot->wider pop3 "variable" "value")
;; => data/world_bank_pop.csv.gz [9504 5]:
;;    | country | area | year |    GROW |      TOTL |
;;    |---------+------+------+---------+-----------|
;;    |     ABW |  URB | 2013 |  0.6695 | 4.436E+04 |
;;    |     ABW |  POP | 2013 |  0.5929 | 1.032E+05 |
;;    |     AFG |  URB | 2013 |   4.193 | 7.734E+06 |
;;    |     AFG |  POP | 2013 |   3.315 | 3.173E+07 |
;;    |     AGO |  URB | 2013 |   4.723 | 1.612E+07 |
;;    |     AGO |  POP | 2013 |   3.532 | 2.600E+07 |
;;    |     ALB |  URB | 2013 |   1.744 | 1.604E+06 |
;;    |     ALB |  POP | 2013 | -0.1832 | 2.895E+06 |
;;    |     AND |  URB | 2013 |  -2.119 | 7.153E+04 |
;;    |     AND |  POP | 2013 |  -2.013 | 8.079E+04 |
;;    |     ARB |  URB | 2013 |   2.783 | 2.186E+08 |
;;    |     ARB |  POP | 2013 |   2.249 | 3.817E+08 |
;;    |     ARE |  URB | 2013 |   1.555 | 7.661E+06 |
;;    |     ARE |  POP | 2013 |   1.182 | 9.006E+06 |
;;    |     ARG |  URB | 2013 |   1.188 | 3.882E+07 |
;;    |     ARG |  POP | 2013 |   1.047 | 4.254E+07 |
;;    |     ARM |  URB | 2013 |  0.2810 | 1.828E+06 |
;;    |     ARM |  POP | 2013 |  0.4013 | 2.894E+06 |
;;    |     ASM |  URB | 2013 | 0.05798 | 4.831E+04 |
;;    |     ASM |  POP | 2013 |  0.1393 | 5.531E+04 |
;;    |     ATG |  URB | 2013 |  0.3838 | 2.480E+04 |
;;    |     ATG |  POP | 2013 |   1.076 | 9.782E+04 |
;;    |     AUS |  URB | 2013 |   1.875 | 1.979E+07 |
;;    |     AUS |  POP | 2013 |   1.758 | 2.315E+07 |
;;    |     AUT |  URB | 2013 |  0.9196 | 4.862E+06 |

(def multi (dataset {:id [1 2 3 4]
                     :choice1 ["A" "C" "D" "B"]
                     :choice2 ["B" "B" nil "D"]
                     :choice3 ["C" nil nil nil]}))

(def multi2 (-> multi
                (pivot->longer (complement #{:id}))
                (add-or-update-column :checked true)))
multi2

;; => _unnamed [8 4]:
;;    | :id | :$column | :$value | :checked |
;;    |-----+----------+---------+----------|
;;    |   1 | :choice1 |       A |     true |
;;    |   2 | :choice1 |       C |     true |
;;    |   3 | :choice1 |       D |     true |
;;    |   4 | :choice1 |       B |     true |
;;    |   1 | :choice2 |       B |     true |
;;    |   2 | :choice2 |       B |     true |
;;    |   4 | :choice2 |       D |     true |
;;    |   1 | :choice3 |       C |     true |

(-> multi2
    (drop-columns :$column)
    (pivot->wider :$value :checked {:drop-missing? false}))

;; => _unnamed [4 5]:
;;    | :id |    A |    B |    C |    D |
;;    |-----+------+------+------+------|
;;    |   3 |      |      |      | true |
;;    |   4 |      | true |      | true |
;;    |   1 | true | true | true |      |
;;    |   2 |      | true | true |      |


(def construction (dataset "data/construction.csv"))

(def construction-unit-map {"1 unit" "1"
                            "2 to 4 units" "2-4"
                            "5 units or more" "5+"})

(-> construction
    (pivot->longer #"^[125NWS].*|Midwest" {:target-cols [:units :region]
                                           :splitter (fn [col-name]
                                                       (if (re-matches #"^[125].*" col-name)
                                                         [(construction-unit-map col-name) nil]
                                                         [nil col-name]))
                                           :value-column-name :n
                                           :drop-missing? false})
    (select-rows (fn [row] (and (= "January" (row "Month"))))))

;; => data/construction.csv [7 5]:
;;    | Year |   Month | :units |   :region |  :n |
;;    |------+---------+--------+-----------+-----|
;;    | 2018 | January |      1 |           | 859 |
;;    | 2018 | January |    2-4 |           |     |
;;    | 2018 | January |     5+ |           | 348 |
;;    | 2018 | January |        | Northeast | 114 |
;;    | 2018 | January |        |   Midwest | 169 |
;;    | 2018 | January |        |     South | 596 |
;;    | 2018 | January |        |      West | 339 |

(-> construction
    (pivot->longer #"^[125NWS].*|Midwest" {:target-cols [:units :region]
                                           :splitter (fn [col-name]
                                                       (if (re-matches #"^[125].*" col-name)
                                                         [(construction-unit-map col-name) nil]
                                                         [nil col-name]))
                                           :value-column-name :n
                                           :drop-missing? false})
    (pivot->wider [:units :region] :n {:rename-map (zipmap (vals construction-unit-map)
                                                           (keys construction-unit-map))}))

;; => data/construction.csv [9 9]:
;;    | Year |     Month | Midwest | 5 units or more | 2 to 4 units | Northeast | South | 1 unit | West |
;;    |------+-----------+---------+-----------------+--------------+-----------+-------+--------+------|
;;    | 2018 |   January |     169 |             348 |              |       114 |   596 |    859 |  339 |
;;    | 2018 |  February |     160 |             400 |              |       138 |   655 |    882 |  336 |
;;    | 2018 |     March |     154 |             356 |              |       150 |   595 |    862 |  330 |
;;    | 2018 |     April |     196 |             447 |              |       144 |   613 |    797 |  304 |
;;    | 2018 |       May |     169 |             364 |              |        90 |   673 |    875 |  319 |
;;    | 2018 |      June |     170 |             342 |              |        76 |   610 |    867 |  360 |
;;    | 2018 |      July |     183 |             360 |              |       108 |   594 |    829 |  310 |
;;    | 2018 |    August |     205 |             286 |              |        90 |   649 |    939 |  286 |
;;    | 2018 | September |     175 |             304 |              |       117 |   560 |    835 |  296 |

;;

(def stockstidyr (dataset "data/stockstidyr.csv"))

(pivot->longer stockstidyr ["X" "Y" "Z"] {:value-column-name :price
                                          :target-cols :stocks})

;; => data/stockstidyr.csv [30 3]:
;;    |       time | :stocks |  :price |
;;    |------------+---------+---------|
;;    | 2009-01-01 |       X |   1.310 |
;;    | 2009-01-02 |       X | -0.2999 |
;;    | 2009-01-03 |       X |  0.5365 |
;;    | 2009-01-04 |       X |  -1.884 |
;;    | 2009-01-05 |       X | -0.9605 |
;;    | 2009-01-06 |       X |  -1.185 |
;;    | 2009-01-07 |       X | -0.8521 |
;;    | 2009-01-08 |       X |  0.2523 |
;;    | 2009-01-09 |       X |  0.4026 |
;;    | 2009-01-10 |       X | -0.6438 |
;;    | 2009-01-01 |       Y |  -1.890 |
;;    | 2009-01-02 |       Y |  -1.825 |
;;    | 2009-01-03 |       Y |  -1.036 |
;;    | 2009-01-04 |       Y | -0.5218 |
;;    | 2009-01-05 |       Y |  -2.217 |
;;    | 2009-01-06 |       Y |  -2.894 |
;;    | 2009-01-07 |       Y |  -2.168 |
;;    | 2009-01-08 |       Y | -0.3285 |
;;    | 2009-01-09 |       Y |   1.964 |
;;    | 2009-01-10 |       Y |   2.686 |
;;    | 2009-01-01 |       Z |  -1.779 |
;;    | 2009-01-02 |       Z |   2.399 |
;;    | 2009-01-03 |       Z |  -3.987 |
;;    | 2009-01-04 |       Z |  -2.831 |
;;    | 2009-01-05 |       Z |   1.437 |

(def iris (dataset "data/iris.csv"))

(def mini-iris (select-rows iris [1 50 100]))

(pivot->longer mini-iris (complement #{"Species"}) {:value-column-name :measurement
                                                    :target-cols "flower_att"})

;; => data/iris.csv [12 3]:
;;    |    Species |   flower_att | :measurement |
;;    |------------+--------------+--------------|
;;    |     setosa | Sepal.Length |        4.900 |
;;    | versicolor | Sepal.Length |        7.000 |
;;    |  virginica | Sepal.Length |        6.300 |
;;    |     setosa |  Sepal.Width |        3.000 |
;;    | versicolor |  Sepal.Width |        3.200 |
;;    |  virginica |  Sepal.Width |        3.300 |
;;    |     setosa | Petal.Length |        1.400 |
;;    | versicolor | Petal.Length |        4.700 |
;;    |  virginica | Petal.Length |        6.000 |
;;    |     setosa |  Petal.Width |       0.2000 |
;;    | versicolor |  Petal.Width |        1.400 |
;;    |  virginica |  Petal.Width |        2.500 |
;;

(def stocksm (pivot->longer stockstidyr ["X" "Y" "Z"] {:value-column-name :price
                                                       :target-cols :stocks}))

(pivot->wider stocksm :stocks :price)

;; => data/stockstidyr.csv [10 4]:
;;    |       time |      Z |       X |       Y |
;;    |------------+--------+---------+---------|
;;    | 2009-01-01 | -1.779 |   1.310 |  -1.890 |
;;    | 2009-01-02 |  2.399 | -0.2999 |  -1.825 |
;;    | 2009-01-03 | -3.987 |  0.5365 |  -1.036 |
;;    | 2009-01-04 | -2.831 |  -1.884 | -0.5218 |
;;    | 2009-01-05 |  1.437 | -0.9605 |  -2.217 |
;;    | 2009-01-06 |  3.398 |  -1.185 |  -2.894 |
;;    | 2009-01-07 | -1.201 | -0.8521 |  -2.168 |
;;    | 2009-01-08 | -1.532 |  0.2523 | -0.3285 |
;;    | 2009-01-09 | -6.809 |  0.4026 |   1.964 |
;;    | 2009-01-10 | -2.559 | -0.6438 |   2.686 |

(pivot->wider (select-rows stocksm (range 0 30 4)) "time" :price)

;; => data/stockstidyr.csv [3 6]:
;;    | :stocks | 2009-01-05 | 2009-01-07 | 2009-01-01 | 2009-01-03 | 2009-01-09 |
;;    |---------+------------+------------+------------+------------+------------|
;;    |       X |    -0.9605 |            |      1.310 |            |     0.4026 |
;;    |       Z |      1.437 |            |     -1.779 |            |     -6.809 |
;;    |       Y |            |     -2.168 |            |     -1.036 |            |

(def df (dataset {:x ["a" "b"]
                  :y [3 4]
                  :z [5 6]}))

df
;; => _unnamed [2 3]:
;;    | :x | :y | :z |
;;    |----+----+----|
;;    |  a |  3 |  5 |
;;    |  b |  4 |  6 |

(pivot->wider df :x :y)

;; => _unnamed [2 3]:
;;    | :z | b | a |
;;    |----+---+---|
;;    |  5 |   | 3 |
;;    |  6 | 4 |   |

(-> (pivot->wider df :x :y)
    (pivot->longer ["a" "b"] {:target-cols :x
                              :value-column-name :y}))

;; => _unnamed [2 3]:
;;    | :z | :x | :y |
;;    |----+----+----|
;;    |  5 |  a |  3 |
;;    |  6 |  b |  4 |

