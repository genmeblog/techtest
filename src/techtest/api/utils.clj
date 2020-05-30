(ns techtest.api.utils
  (:require [tech.ml.dataset :as ds]))

;;;;;;;;;;;;
;; HELPERS
;;;;;;;;;;;;

(defn iterable-sequence?
  "Check if object is sequential, is column or maybe a reader (iterable)?"
  [xs]
  (or (sequential? xs)      
      (and (not (map? xs))
           (instance? Iterable xs))))

(defn ->str
  [v]
  (if (instance? clojure.lang.Named v) (name v) (str v)))

(defn rank
  "Sample ranks. See [R docs](https://www.rdocumentation.org/packages/base/versions/3.6.1/topics/rank).
  Rank uses 0 based indexing.
  
  Possible tie strategies: `:average`, `:first`, `:last`, `:random`, `:min`, `:max`, `:dense`.
  `:dense` is the same as in `data.table::frank` from R"
  ([vs] (rank vs :average))
  ([vs ties] (rank vs ties false))
  ([vs ties desc?]
   (let [cmp (if desc? #(compare %2 %1) compare)
         indexed-sorted-map (group-by second (map-indexed vector (sort cmp vs)))]
     (if (#{:first :last :random} ties)
       (let [tie-sort (case ties
                        :first (partial sort-by first clojure.core/<)
                        :last (partial sort-by first clojure.core/>)
                        :random shuffle)
             sorted2-map (into {} (map (fn [[k v]] [k (tie-sort v)]) indexed-sorted-map))]
         (first (reduce (fn [[res curr] v]
                          (let [lst (curr v)]
                            [(conj res (ffirst lst))
                             (assoc curr v (rest lst))])) [[] sorted2-map] vs)))
       (let [tie-fn (case ties
                      :min ffirst
                      :dense ffirst
                      :max (comp first last)
                      (fn ^double [v] (/ ^double (reduce #(+ ^double %1 ^double %2) (map first v)) (count v))))
             m (map (fn [[k v]] [k (tie-fn v)]) indexed-sorted-map)
             m (if (= ties :dense)
                 (map-indexed (fn [id [k _]]
                                [k id]) (sort-by second m))
                 m)]
         (map (into {} m) vs))))))

;;

(defn- filter-column-names
  "Filter column names"
  [ds columns-selector meta-field]
  (let [field-fn (if (= :all meta-field)
                   identity
                   (or meta-field :name))]
    (->> ds
         (ds/columns)
         (map meta)
         (filter (comp columns-selector field-fn))
         (map :name))))

(defn column-names
  ([ds] (ds/column-names ds))
  ([ds columns-selector] (column-names ds columns-selector :name))
  ([ds columns-selector meta-field]
   (if (= :all columns-selector)
     (ds/column-names ds)
     (let [csel-fn (cond
                     (map? columns-selector) (set (keys columns-selector))
                     (iterable-sequence? columns-selector) (set columns-selector)
                     (instance? java.util.regex.Pattern columns-selector) #(re-matches columns-selector (str %))
                     (fn? columns-selector) columns-selector
                     :else #{columns-selector})]
       (filter-column-names ds csel-fn meta-field)))))
