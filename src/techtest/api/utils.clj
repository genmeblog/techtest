(ns techtest.api.utils)

;;;;;;;;;;;;
;; HELPERS
;;;;;;;;;;;;

(defn map-v [f coll] (reduce-kv (fn [m k v] (assoc m k (f v))) (empty coll) coll))
(defn map-kv [f coll] (reduce-kv (fn [m k v] (assoc m k (f k v))) (empty coll) coll))

(defn iterable-sequence?
  "Check if object is sequential, is column or maybe a reader (iterable)?"
  [xs]
  (or (sequential? xs)      
      (and (not (map? xs))
           (instance? Iterable xs))))
