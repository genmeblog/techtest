(ns techtest.api.join-concat-ds
  (:refer-clojure :exclude [concat])
  (:require [tech.ml.dataset :as ds]

            [techtest.api.join-separate :refer [join-columns]]
            [techtest.api.columns :refer [column-names]]))

;; joins

(defn- multi-join
  [ds-left ds-right join-fn cols options]
  (let [join-column-name (gensym "^___join_column_hash")
        dsl (join-columns ds-left join-column-name cols {:result-type hash
                                                         :drop-columns? false})
        dsr (join-columns ds-right join-column-name cols {:result-type hash
                                                          :drop-columns? false})
        joined-ds (join-fn join-column-name dsl dsr options)]
    (-> joined-ds
        (ds/drop-columns [join-column-name (-> joined-ds
                                               (meta)
                                               :right-column-names
                                               (get join-column-name))]))))

(defmacro make-join-fns
  [join-fns-list]
  `(do
     ~@(for [[n impl] join-fns-list]
         `(defn ~n
            ([~'ds-left ~'ds-right ~'columns-selector] (~n ~'ds-left ~'ds-right ~'columns-selector nil))
            ([~'ds-left ~'ds-right ~'columns-selector ~'options]
             (let [cols# (column-names ~'ds-left ~'columns-selector)
                   opts# (or ~'options {})]
               (if (= 1 (count cols#))
                 (~impl (first cols#) ~'ds-left ~'ds-right opts#)
                 (multi-join ~'ds-left ~'ds-right ~impl cols# opts#))))))))

(make-join-fns [[left-join ds/left-join]
                [right-join ds/right-join]
                [inner-join ds/inner-join]])
