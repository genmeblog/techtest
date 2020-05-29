(ns techtest.api.join-concat-ds
  (:refer-clojure :exclude [concat])
  (:require [tech.ml.dataset :as ds]

            [techtest.api.join-separate :refer [join-columns]]
            [techtest.api.missing :refer [select-missing drop-missing]]
            [techtest.api.columns :refer [column-names drop-columns]]))

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


(defn full-join
  ([ds-left ds-right columns-selector] (full-join ds-left ds-right columns-selector nil))
  ([ds-left ds-right columns-selector options]
   (let [rj (right-join ds-left ds-right columns-selector options)]
     (-> (->> rj
              (ds/concat (left-join ds-left ds-right columns-selector options)))
         (with-meta (assoc (meta rj) :name "full-join"))))))

(defn semi-join
  ([ds-left ds-right columns-selector] (semi-join ds-left ds-right columns-selector nil))
  ([ds-left ds-right columns-selector options]
   (let [lj (left-join ds-left ds-right columns-selector options)]
     (-> (->> (-> lj
                  (drop-missing)
                  (drop-columns (vals (:right-column-names (meta lj)))))
              (ds/unique-by identity))
         (vary-meta assoc :name "semi-join")))))

(defn anti-join
  ([ds-left ds-right columns-selector] (anti-join ds-left ds-right columns-selector nil))
  ([ds-left ds-right columns-selector options]
   (let [lj (left-join ds-left ds-right columns-selector options)]
     (-> (->> (-> lj
                  (select-missing)
                  (drop-columns (vals (:right-column-names (meta lj)))))
              (ds/unique-by identity))
         (vary-meta assoc :name "anit-join")))))
