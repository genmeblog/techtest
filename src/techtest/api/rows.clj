(ns techtest.api.rows
  (:refer-clojure :exclude [shuffle rand-nth first last])
  (:require [tech.ml.dataset :as ds]

            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.columns :refer [add-or-update-columns]]
            [techtest.api.group-by :refer [grouped? process-group-data]]
            [tech.v2.datatype.functional :as dfn]))

(defn- find-indexes-from-seq
  "Find row indexes based on true/false values or indexes"
  [ds rows-selector]
  (if (number? (clojure.core/first rows-selector))
    rows-selector
    (->> rows-selector
         (take (ds/row-count ds))
         (dfn/argfilter identity))))

(defn- find-indexes-from-fn
  "Filter rows"
  [ds rows-selector limit-columns]
  (->> (or limit-columns :all)
       (ds/select-columns ds)
       (ds/mapseq-reader)
       (dfn/argfilter rows-selector)))

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
  ([f ds rows-selector {:keys [limit-columns pre]}]
   (if (grouped? ds)
     (let [pre-ds (map #(add-or-update-columns % pre) (ds :data))
           indices (map #(find-indexes % rows-selector limit-columns) pre-ds)]       
       (ds/add-or-update-column ds :data (map #(select-or-drop-rows f %1 %2) (ds :data) indices)))
     
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

;;

(defn head
  ([ds] (head ds 5))
  ([ds n]
   (if (grouped? ds)
     (process-group-data ds (partial ds/head n))
     (ds/head n ds))))

(defn tail
  ([ds] (tail ds 5))
  ([ds n]
   (if (grouped? ds)
     (process-group-data ds (partial ds/tail n))
     (ds/tail n ds))))

(defn shuffle
  [ds]
  (if (grouped? ds)
    (process-group-data ds ds/shuffle)
    (ds/shuffle ds)))

(defn random
  ([ds] (random ds (ds/row-count ds)))
  ([ds n] (random ds n nil))
  ([ds n {:keys [repeat?]
          :or {repeat? true}
          :as options}]
   (if (grouped? ds)
     (process-group-data ds #(random % n options))
     (let [cnt (ds/row-count ds)
           idxs (if repeat?
                  (repeatedly n #(int (rand cnt)))
                  (take (min cnt n) (clojure.core/shuffle (range cnt))))]
       (ds/select-rows ds idxs)))))

(defn rand-nth
  [ds]
  (random ds 1))

(defn first
  [ds]
  (if (grouped? ds)
    (process-group-data ds #(ds/select-rows % [0]))
    (ds/select-rows ds [0])))

(defn last
  [ds]
  (if (grouped? ds)
    (process-group-data ds last)
    (let [idx (dec (ds/row-count ds))]
      (ds/select-rows ds [idx]))))
