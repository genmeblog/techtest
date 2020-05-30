(ns techtest.api.columns
  (:refer-clojure :exclude [group-by])
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype :as dtype]

            [techtest.api.utils :refer [column-names iterable-sequence?]]
            [techtest.api.dataset :refer [dataset]]
            [techtest.api.group-by :refer [grouped? process-group-data]]))

(defn- select-or-drop-columns
  "Select or drop columns."
  ([f rename-enabled? ds] (select-or-drop-columns f rename-enabled? ds :all))
  ([f rename-enabled? ds columns-selector] (select-or-drop-columns f rename-enabled? ds columns-selector nil))
  ([f rename-enabled? ds columns-selector meta-field]
   (if (grouped? ds)
     (process-group-data ds #(select-or-drop-columns f rename-enabled? % columns-selector))
     (let [nds (f ds (column-names ds columns-selector meta-field))]
       (if (and rename-enabled? (map? columns-selector))
         (ds/rename-columns nds columns-selector)
         nds)))))

(defn- select-or-drop-colums-docstring
  [op]
  (str op " columns by (returns dataset):

  - name
  - sequence of names
  - map of names with new names (rename)
  - function which filter names (via column metadata)"))

(def ^{:doc (select-or-drop-colums-docstring "Select")}
  select-columns (partial select-or-drop-columns ds/select-columns true))

(def ^{:doc (select-or-drop-colums-docstring "Drop")}
  drop-columns (partial select-or-drop-columns ds/drop-columns false))

;;

(defn- get-cols-mapping
  [ds columns-selector columns-mapping]
  (if (fn? columns-mapping)
    (let [ds (if (grouped? ds)
               (first (ds :data))
               ds)
          col-names (column-names ds columns-selector)]
      (->> (map columns-mapping col-names)
           (map vector col-names)
           (remove (comp nil? second))
           (into {})))
    columns-mapping))

(defn rename-columns
  "Rename columns with provided old -> new name map"
  ([ds columns-selector columns-map-fn] (rename-columns ds (get-cols-mapping ds columns-selector columns-map-fn)))
  ([ds columns-mapping]
   (let [colmap (get-cols-mapping ds :all columns-mapping)]
     (if (grouped? ds)
       (process-group-data ds #(ds/rename-columns % colmap))
       (ds/rename-columns ds colmap)))))

;;

(defn- cycle-column
  [column col-cnt cnt]
  (let [q (quot cnt col-cnt)
        r (rem cnt col-cnt)
        col-name (col/column-name column)
        tmp-ds (->> (dataset [column])
                    (repeat q)
                    (apply ds/concat))]
    (if (zero? r)
      (tmp-ds col-name)
      ((-> (ds/concat tmp-ds
                      (dataset [(dtype/sub-buffer column 0 r)]))) col-name))))

(defn- fix-column-size-column
  [column strategy cnt]
  (let [ec (dtype/ecount column)]
    (when (and (= :strict strategy)
               (not= cnt ec))
      (throw (Exception. (str "Column size (" ec ") should be exactly the same as dataset row count (" cnt ")"))))
    (cond
      (> ec cnt) (dtype/sub-buffer column 0 cnt)
      (< ec cnt) (if (= strategy :cycle)
                   (cycle-column column ec cnt)
                   (col/extend-column-with-empty column (- cnt ec)))
      :else column)))


(defn- fix-column-size-seq
  [column strategy cnt]
  (let [column (take cnt column)
        seq-cnt (count column)]
    (when (and (= :strict strategy)
               (not= cnt seq-cnt))
      (throw (Exception. (str "Sequence size (" seq-cnt ") should be exactly the same as dataset row count (" cnt ")"))))
    (if (< seq-cnt cnt)
      (if (= strategy :cycle)
        (take cnt (cycle column))
        (clojure.core/concat column (repeat (- cnt seq-cnt) nil)))
      column)))

(defn- fix-column-size
  [f ds column-name column strategy]
  (->> (ds/row-count ds)
       (f column strategy)
       (ds/add-or-update-column ds column-name)))

(declare add-or-update-column)

(defn- prepare-add-or-update-column-fn
  [column-name column size-strategy]
  (cond
    (col/is-column? column) #(fix-column-size fix-column-size-column % column-name column size-strategy)
    (iterable-sequence? column) #(fix-column-size fix-column-size-seq % column-name column size-strategy)
    (fn? column) #(add-or-update-column % column-name (column %) size-strategy)
    :else #(ds/add-or-update-column % column-name (repeat (ds/row-count %) column))))

(defn add-or-update-column
  "Add or update (modify) column under `column-name`.

  `column` can be sequence of values or generator function (which gets `ds` as input)."
  ([ds column-name column] (add-or-update-column ds column-name column nil))
  ([ds column-name column size-strategy]
   (let [process-fn (prepare-add-or-update-column-fn column-name column (or size-strategy :cycle))]
     
     (if (grouped? ds)
       (process-group-data ds process-fn)
       (process-fn ds)))))

(defn add-or-update-columns
  "Add or updade (modify) columns defined in `columns-map` (mapping: name -> column) "
  ([ds columns-map] (add-or-update-columns ds columns-map nil))
  ([ds columns-map size-strategy]
   (reduce-kv (fn [ds k v] (add-or-update-column ds k v size-strategy)) ds columns-map)))

(defn map-columns
  [ds column-name columns-selector map-fn]
  (if (grouped? ds)
    (process-group-data ds #(map-columns % column-name columns-selector map-fn))
    (apply ds/column-map ds column-name map-fn (when columns-selector (column-names ds columns-selector)))))

(defn reorder-columns
  "Reorder columns using column selector(s). When column names are incomplete, the missing will be attached at the end."
  [ds columns-selector & columns-selectors]
  (let [selected-cols (->> columns-selectors
                           (map (partial column-names ds))
                           (apply clojure.core/concat (column-names ds columns-selector)))
        rest-cols (column-names ds (complement (set selected-cols)))]
    (ds/select-columns ds (clojure.core/concat selected-cols rest-cols))))
;;

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

(defn ->array
  "Convert numerical column(s) to java array"
  ([ds colname] (->array ds colname nil))
  ([ds colname datatype]
   (if (grouped? ds)
     (map  #(->array % colname datatype) (ds :data))
     (let [c (ds colname)]
       (if (and datatype (not= datatype (dtype/get-datatype c)))
         (dtype/make-array-of-type datatype c)
         (dtype/->array-copy c))))))


