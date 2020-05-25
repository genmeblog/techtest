(ns techtest.api.join-separate
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [clojure.string :as str]

            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.group-by :refer [grouped? process-group-data]]
            [techtest.api.columns :refer [select-columns column-names drop-columns]]))

(defn- process-join-columns
  [ds target-column join-function col-names options drop-columns?]
  (let [cols (select-columns ds col-names)
        result (ds/add-or-update-column ds target-column (->> (ds/value-reader cols options)
                                                              (map join-function)))]
    (if drop-columns? (drop-columns result col-names) result)))

(defn join-columns
  ([ds target-column columns-selector] (join-columns ds target-column columns-selector nil))
  ([ds target-column columns-selector {:keys [separator missing-subst drop-columns? result-type]
                                       :or {separator "-" drop-columns? true result-type :string}
                                       :as options}]
   
   (let [missing-subst-fn #(map (fn [v] (or v missing-subst)) %)
         col-names (column-names (if (grouped? ds)
                                   (first (ds :data))
                                   ds) columns-selector)
         join-function (comp (cond
                               (= :map result-type) #(zipmap col-names %)
                               (= :seq result-type) seq
                               (fn? result-type) result-type
                               :else (if (iterable-sequence? separator)
                                       (let [sep (concat
                                                  (conj (seq separator) :empty)
                                                  (cycle separator))]
                                         (fn [row] (->> row
                                                       (remove nil?)
                                                       (interleave sep)
                                                       (rest)
                                                       (apply str))))
                                       (fn [row] (->> row
                                                     (remove nil?)
                                                     (str/join separator)))))
                             missing-subst-fn)]

     (if (grouped? ds)
       (process-group-data ds #(process-join-columns % target-column join-function col-names options drop-columns?))
       (process-join-columns ds target-column join-function col-names options drop-columns?)))))

;;

(defn- separate-column->columns
  [col target-columns replace-missing separator-fn]
  (let [res (map separator-fn col)]
    (remove nil? (map (fn [idx colname]
                        (when colname
                          (let [{:keys [data missing]} (col/scan-data-for-missing
                                                        (map #(replace-missing (nth % idx)) res))]
                            (col/new-column colname data nil missing)))) (range) target-columns))))

(defn- prepare-missing-subst-fn
  [missing-subst]
  (let [missing-subst-fn (cond
                           (or (set? missing-subst)
                               (fn? missing-subst)) missing-subst
                           (iterable-sequence? missing-subst) (set missing-subst)
                           :else (partial = missing-subst))]
    (fn [v] (if (missing-subst-fn v) nil v))))

(defn- process-separate-columns
  [ds column target-columns replace-missing separator-fn drop-column?]
  (let [result (separate-column->columns (ds column) target-columns replace-missing separator-fn)
        [dataset-before dataset-after] (map (partial ds/select-columns ds)
                                            (split-with #(not= % column)
                                                        (ds/column-names ds)))]
    (-> (if drop-column?
          dataset-before
          (ds/add-column dataset-before (ds column)))
        (ds/append-columns result)
        (ds/append-columns (ds/columns (ds/drop-columns dataset-after [column]))))))

(defn separate-column
  ([ds column target-columns] (separate-column ds column target-columns identity))
  ([ds column target-columns separator] (separate-column ds column target-columns separator nil))
  ([ds column target-columns separator {:keys [missing-subst drop-column?]
                                        :or {missing-subst "" drop-column? true}}]
   (let [separator-fn (cond
                        (string? separator) (let [pat (re-pattern separator)]
                                              #(-> (str %)
                                                   (str/split pat)
                                                   (concat (repeat ""))))
                        (instance? java.util.regex.Pattern separator) #(-> separator
                                                                           (re-matches (str %))
                                                                           (rest)
                                                                           (concat (repeat "")))
                        :else separator)
         replace-missing (if missing-subst
                           (prepare-missing-subst-fn missing-subst)
                           identity)]
     
     (if (grouped? ds)       
       (process-group-data ds #(process-separate-columns % column target-columns replace-missing separator-fn drop-column?))
       (process-separate-columns ds column target-columns replace-missing separator-fn drop-column?)))))
