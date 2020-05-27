(ns techtest.api.unique-by
  (:require [tech.ml.dataset :as ds]
            
            [techtest.api.utils :refer [iterable-sequence?]]
            [techtest.api.dataset :refer [dataset]]
            [techtest.api.columns :refer [select-columns column-names]]
            [techtest.api.group-by :refer [grouped? process-group-data ungroup]]))

(defn- strategy-first [_ idxs] (clojure.core/first idxs))
(defn- strategy-last [_ idxs] (clojure.core/last idxs))
(defn- strategy-random [_ idxs] (clojure.core/rand-nth idxs))

(def ^:private strategies
  {:first strategy-first
   :last strategy-last
   :random strategy-random})

(defn strategy-fold
  ([ds columns-selector] (strategy-fold ds columns-selector nil))
  ([ds columns-selector fold-fn] (strategy-fold ds columns-selector fold-fn nil))
  ([ds columns-selector fold-fn ungroup-options]
   (let [[group-by-selector target-names] (if (fn? columns-selector)
                                            [columns-selector (ds/column-names ds)]
                                            (let [group-by-names (column-names ds columns-selector)]
                                              [group-by-names (column-names ds (complement (set group-by-names)))]))
         fold-fn (or fold-fn vec)]
     (-> (techtest.api.group-by/group-by ds group-by-selector)
         (process-group-data (fn [ds]
                               (as-> ds ds
                                 (select-columns ds target-names)
                                 (dataset [(zipmap target-names (map fold-fn (ds/columns ds)))]))))
         (ungroup ungroup-options)))))

(defn- unique-by-fn
  [strategy columns-selector limit-columns options]
  (if (fn? strategy)

    (fn [ds] (strategy-fold ds columns-selector strategy options))
    
    (let [local-options {:keep-fn (get strategies strategy strategy-first)}]
      (cond
        (iterable-sequence? columns-selector) (let [local-options (assoc local-options :column-name-seq columns-selector)]
                                                (fn [ds]
                                                  (if (= (count columns-selector) 1)
                                                    (ds/unique-by-column (clojure.core/first columns-selector) local-options ds)
                                                    (ds/unique-by identity local-options ds))))
        (fn? columns-selector) (let [local-options (if limit-columns
                                                     (assoc local-options :column-name-seq limit-columns)
                                                     local-options)]
                                 (fn [ds] (ds/unique-by columns-selector local-options ds)))
        :else (fn [ds] (ds/unique-by-column columns-selector local-options ds))))))

(defn- maybe-skip-unique
  [ds ufn]
  (if (= 1 (ds/row-count ds))
    ds
    (ufn ds)))

(defn unique-by
  ([ds] (unique-by ds (ds/column-names ds)))
  ([ds columns-selector] (unique-by ds columns-selector nil))
  ([ds columns-selector {:keys [strategy limit-columns]
                         :or {strategy :first}
                         :as options}]

   (let [ufn (unique-by-fn strategy columns-selector limit-columns options)]

     (if (grouped? ds)
       (process-group-data ds #(maybe-skip-unique % ufn))
       (maybe-skip-unique ds ufn)))))
