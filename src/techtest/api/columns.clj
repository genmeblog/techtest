(ns techtest.api.columns
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype :as dtype]

            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.group-by :refer [grouped? process-group-data]]))

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

(defn select-column-names
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
