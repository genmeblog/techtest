(ns techtest.api.utils)

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

