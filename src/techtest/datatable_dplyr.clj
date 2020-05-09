(ns techtest.datatable-dplyr
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype :as dtype]
            [fastmath.core :as m]
            [clojure.string :as str]))

;; Comparizon of tech.ml.dataset with datatable and dplyr
;; Based on article "A data.table and dplyr tour" by Atrebas dated March 3, 2019
;; https://atrebas.github.io/post/2019-03-03-datatable-dplyr/

;; Dataset names used:
;;
;; DT - R data.table object
;; DF - R tibble (dplyr) object
;; DS - tech.ml.dataset object

;; # Preparation

;; load R package

(require '[clojisr.v1.r :as r :refer [r r->clj]]
         '[clojisr.v1.require :refer [require-r]])

(require-r '[base]
           '[utils]
           '[stats]
           '[tidyr]
           '[dplyr :as dpl]
           '[data.table :as dt])
(r.base/options :width 160)
(r.base/set-seed 1)

;; HELPER functions

(defn aggregate
  ([agg-fns-map ds]
   (aggregate {} agg-fns-map ds))
  ([m agg-fns-map-or-vector ds]
   (into m (map (fn [[k agg-fn]]
                  [k (agg-fn ds)]) (if (map? agg-fns-map-or-vector)
                                     agg-fns-map-or-vector
                                     (map-indexed vector agg-fns-map-or-vector))))))

(def aggregate->dataset (comp ds/->dataset vector aggregate))

(defn group-by-columns-or-fn-and-aggregate
  [key-fn-or-gr-colls agg-fns-map ds]
  (->> (if (fn? key-fn-or-gr-colls)
         (ds/group-by key-fn-or-gr-colls ds)
         (ds/group-by identity key-fn-or-gr-colls ds))
       (map (fn [[group-idx group-ds]]
              (let [group-idx-m (if (map? group-idx)
                                  group-idx
                                  {:_fn group-idx})]
                (aggregate group-idx-m agg-fns-map group-ds))))
       ds/->dataset))

(defn asc-desc-comparator
  [orders]
  (if (every? #(= % :asc) orders)
    compare
    (let [mults (map #(if (= % :asc) 1 -1) orders)]
      (fn [v1 v2]
        (reduce (fn [_ [a b ^long mult]]
                  (let [c (compare a b)]
                    (if-not (zero? c)
                      (reduced (* mult c))
                      c))) 0 (map vector v1 v2 mults))))))

(defn sort-by-columns-with-orders
  ([cols ds]
   (sort-by-columns-with-orders cols (repeat (count cols) :asc) ds))
  ([cols orders ds]
   (let [sel (apply juxt (map #(fn [ds] (get ds %)) cols))
         comp-fn (asc-desc-comparator orders)]
     (ds/sort-by sel comp-fn ds))))

(defn map-v [f coll]
  (reduce-kv (fn [m k v] (assoc m k (f v))) (empty coll) coll))

(defn filter-by-external-values->indices
  [pred values]
  (->> values
       (map-indexed vector)
       (filter (comp pred second))
       (map first)))

(defn my-max [xs] (reduce #(if (pos? (compare %1 %2)) %1 %2) xs))
(defn my-min [xs] (reduce #(if (pos? (compare %2 %1)) %1 %2) xs))

(defn- ensure-seq
  [v]
  (if (seqable? v) v [v]))

(defn apply-to-columns
  [f columns ds]
  (let [names (ds/column-names ds)
        columns (set (if (= :all columns) names columns))
        ff (fn [col]
             (if (columns (col/column-name col))
               (f col)
               col))]
    (->> names
         (map (comp ensure-seq ff ds))
         (zipmap names)
         (ds/name-values-seq->dataset))))

;; # Create example data

;; ---- data.table

;; DT <- data.table(V1 = rep(c(1L, 2L), 5)[-10],
;;                     V2 = 1:9,
;;                     V3 = c(0.5, 1.0, 1.5),
;;                     V4 = rep(LETTERS[1:3], 3))

;; class(DT)
;; DT

(def DT (dt/data-table :V1 (r/bra (r.base/rep [1 2] 5) -10)
                       :V2 (r/colon 1 9)
                       :V3 [0.5 1.0 1.5]
                       :V4 (r.base/rep (r/bra 'LETTERS (r/colon 1 3)) 3)))

(r.base/class DT)
;; => [1] "data.table" "data.frame"
DT
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  2  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  2  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  2  8 1.0  B
;;    9:  1  9 1.5  C

;; ---- dplyr

;; DF <- tibble(V1 = rep(c(1L, 2L), 5)[-10],
;;                 V2 = 1:9,
;;                 V3 = rep(c(0.5, 1.0, 1.5), 3),
;;                 V4 = rep(LETTERS[1:3], 3))

;; class(DF)
;; DF

(def DF (dpl/tibble :V1 (r/bra (r.base/rep [1 2] 5) -10)
                    :V2 (r/colon 1 9)
                    :V3 (r.base/rep [0.5 1.0 1.5] 3)
                    :V4 (r.base/rep (r/bra 'LETTERS (r/colon 1 3)) 3)))


(r.base/class DF)
;; => [1] "tbl_df"     "tbl"        "data.frame"

DF
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     3   1.5 C    
;;    4     2     4   0.5 A    
;;    5     1     5   1   B    
;;    6     2     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     2     8   1   B    
;;    9     1     9   1.5 C

;; ---- tech.ml.dataset

(def DS (ds/name-values-seq->dataset {:V1 (take 9 (cycle [1 2]))
                                      :V2 (range 1 10)
                                      :V3 (take 9 (cycle [0.5 1.0 1.5]))
                                      :V4 (take 9 (cycle [\A \B \C]))}))

(class DS)
;; => tech.ml.dataset.impl.dataset.Dataset
DS
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

;; TODO (tech.ml.dataset): char datatype? (now inferred as Object)
(col/metadata (DS :V4))
;; => {:name :V4, :size 9, :datatype :object}

;; # Basic operations

;; ## Filter rows

;; ### Filter rows using indices

;; ---- data.table

;; DT[3:4,]
;; DT[3:4] # same

;; we use symbolic call here since there is a bug about interpreting R empty symbol
(r '(bra ~DT (colon 3 4) nil))
;; =>    V1 V2  V3 V4
;;    1:  1  3 1.5  C
;;    2:  2  4 0.5  A

(r/bra DT (r/colon 3 4))
;; =>    V1 V2  V3 V4
;;    1:  1  3 1.5  C
;;    2:  2  4 0.5  A

;; ---- dplyr

;; DF[3:4,]
;; slice(DF, 3:4) # same

(r '(bra ~DF (colon 3 4) nil))
;; => # A tibble: 2 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     3   1.5 C    
;;    2     2     4   0.5 A

(dpl/slice DF (r/colon 3 4))
;; => # A tibble: 2 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     3   1.5 C    
;;    2     2     4   0.5 A

;; ------ tech.ml.datatable

;; NOTE: row ids start at `0`
(ds/select-rows DS [2 3])
;; => _unnamed [2 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |

;; ### Discard rows using negative indices

;; ---- data.table

;; DT[!3:7,]
;; DT[-(3:7)] # same

(r '(bra ~DT (! (colon 3 7)) nil))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  2  8 1.0  B
;;    4:  1  9 1.5  C

(r/bra DT (r/r- (r/colon 3 7)))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  2  8 1.0  B
;;    4:  1  9 1.5  C

;; ---- dplyr

;; DF[-(3:7),]
;; slice(DF, -(3:7)) # same

;; TODO (clojisr): (symbolic) unary `-` on `colon` doesn't work (workaround below)
(r '(bra ~DF (- [(colon 3 7)]) nil))
;; => # A tibble: 4 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     2     8   1   B    
;;    4     1     9   1.5 C

(dpl/slice DF (r/r- (r/colon 3 7)))
;; => # A tibble: 4 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     2     8   1   B    
;;    4     1     9   1.5 C    

;; ---- tech.ml.dataset

(ds/drop-rows DS (range 2 7))
;; => _unnamed [4 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

;; ### Filter rows using a logical expression

;; ---- data.table

;; DT[V2 > 5]
;; DT[V4 %chin% c("A", "C")] # fast %in% for character

(r/bra DT '(> V2 5))
;; =>    V1 V2  V3 V4
;;    1:  2  6 1.5  C
;;    2:  1  7 0.5  A
;;    3:  2  8 1.0  B
;;    4:  1  9 1.5  C

;; TODO (clojisr): add %chin% as binary operation
(r/bra DT '((rsymbol "%chin%") V4 ["A" "C"]))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  1  3 1.5  C
;;    3:  2  4 0.5  A
;;    4:  2  6 1.5  C
;;    5:  1  7 0.5  A
;;    6:  1  9 1.5  C

;; ---- dplyr

;; filter(DF, V2 > 5)
;; filter(DF, V4 %in% c("A", "C"))

(dpl/filter DF '(> V2 5))
;; => # A tibble: 4 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     2     6   1.5 C    
;;    2     1     7   0.5 A    
;;    3     2     8   1   B    
;;    4     1     9   1.5 C

(dpl/filter DF '(%in% V4 ["A" "C"]))
;; => # A tibble: 6 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     1     3   1.5 C    
;;    3     2     4   0.5 A    
;;    4     2     6   1.5 C    
;;    5     1     7   0.5 A    
;;    6     1     9   1.5 C

;; ---- tech.ml.dataset

(ds/filter-column #(> ^long % 5) :V2 DS)
;; => _unnamed [4 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

(ds/filter-column #{\A \C} :V4 DS)
;; => _unnamed [6 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   1 |   9 |  1.500 |   C |

;; ### Filter rows using multiple conditions

;; ---- data.table

;; DT[V1 == 1 & V4 == "A"]

(r/bra DT '(& (== V1 1)
              (== V4 "A")))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  1  7 0.5  A

;; ---- dplyr

;; filter(DF, V1 == 1, V4 == "A")

(dpl/filter DF '(== V1 1) '(== V4 "A"))
;; => # A tibble: 2 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     1     7   0.5 A

;; ---- tech.ml.dataset

(ds/filter #(and (= (:V1 %) 1)
                 (= (:V4 %) \A)) DS)
;; => _unnamed [2 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   1 |   7 | 0.5000 |   A |

;; ### Filter unique rows

;; ---- data.table

;; unique(DT)
;; unique(DT, by = c("V1", "V4")) # returns all cols

(r.base/unique DT)
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  2  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  2  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  2  8 1.0  B
;;    9:  1  9 1.5  C

(r.base/unique DT :by ["V1" "V4"])
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  2  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  2  6 1.5  C

;; ---- dplyr

;; distinct(DF) # distinct_all(DF)
;; distinct_at(DF, vars(V1, V4)) # returns selected cols
;; # see also ?distinct_if

(dpl/distinct DF)
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     3   1.5 C    
;;    4     2     4   0.5 A    
;;    5     1     5   1   B    
;;    6     2     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     2     8   1   B    
;;    9     1     9   1.5 C

(dpl/distinct_at DF (dpl/vars 'V1 'V4))
;; => # A tibble: 6 x 2
;;         V1 V4   
;;      <dbl> <chr>
;;    1     1 A    
;;    2     2 B    
;;    3     1 C    
;;    4     2 A    
;;    5     1 B    
;;    6     2 C    

;; ---- tech.ml.dataset

(ds/unique-by identity {:column-name-seq (ds/column-names DS)} DS)
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

(ds/unique-by identity {:column-name-seq [:V1 :V4]} DS)
;; => _unnamed [6 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |

;; ### Discard rows with missing values

;; ---- data.table

;; na.omit(DT, cols = 1:4)  # fast S3 method with cols argument

(r.stats/na-omit DT :cols (r/colon 1 4))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  2  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  2  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  2  8 1.0  B
;;    9:  1  9 1.5  C

;; ---- dplyr

;; tidyr::drop_na(DF, names(DF))

(r.tidyr/drop_na DF (r.base/names DF))
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     3   1.5 C    
;;    4     2     4   0.5 A    
;;    5     1     5   1   B    
;;    6     2     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     2     8   1   B    
;;    9     1     9   1.5 C    

;; ---- tech.ml.dataset

;; note: missing works on whole dataset, to select columns, first create dataset with selected columns
(ds/drop-rows DS (ds/missing DS))
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

;; ### Other filters

;; ---- data.table

;; DT[sample(.N, 3)] # .N = nb of rows in DT
;; DT[sample(.N, .N / 2)]
;; DT[frankv(-V1, ties.method = "dense") < 2]

(r/bra DT '(sample .N 3))
;; =>    V1 V2  V3 V4
;;    1:  1  7 0.5  A
;;    2:  1  3 1.5  C
;;    3:  2  6 1.5  C

(r/bra DT '(sample .N (/ .N 2)))
;; =>    V1 V2  V3 V4
;;    1:  2  6 1.5  C
;;    2:  1  7 0.5  A
;;    3:  2  4 0.5  A
;;    4:  2  8 1.0  B

(r/bra DT '(< (frankv (- V1) :ties.method "dense") 2))
;; =>    V1 V2  V3 V4
;;    1:  2  2 1.0  B
;;    2:  2  4 0.5  A
;;    3:  2  6 1.5  C
;;    4:  2  8 1.0  B

;; ---- dplyr

;; sample_n(DF, 3)      # n random rows
;; sample_frac(DF, 0.5) # fraction of random rows
;; top_n(DF, 1, V1)     # top n entries (includes equals)

(dpl/sample_n DF 3)
;; => # A tibble: 3 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     5   1   B

(dpl/sample_frac DF 0.5)
;; => # A tibble: 4 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     7   0.5 A    
;;    2     1     3   1.5 C    
;;    3     2     6   1.5 C    
;;    4     2     2   1   B

(dpl/top_n DF 1 'V1)
;; => # A tibble: 4 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     2     2   1   B    
;;    2     2     4   0.5 A    
;;    3     2     6   1.5 C    
;;    4     2     8   1   B    

;; ---- tech.ml.dataset

(ds/sample 3 DS)
;; => _unnamed [3 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |

(ds/sample (/ (ds/row-count DS) 2) DS)
;; => _unnamed [4 4]:
;;    | :V1 | :V2 |   :V3 | :V4 |
;;    |-----+-----+-------+-----|
;;    |   2 |   8 | 1.000 |   B |
;;    |   1 |   5 | 1.000 |   B |
;;    |   2 |   6 | 1.500 |   C |
;;    |   1 |   9 | 1.500 |   C |

;; NOTE: Here we use `fastmath` to calculate rank. 
;;       We need also to translate rank to indices.

(->> (m/rank (map - (DS :V1)) :dense)
     (filter-by-external-values->indices #(< ^long % 1))
     (ds/select-rows DS))
;; => _unnamed [4 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   2 |   2 |  1.000 |   B |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   2 |   6 |  1.500 |   C |
;;    |   2 |   8 |  1.000 |   B |

;; #### Convenience functions

;; ---- data.table

;; DT[V4 %like% "^B"]
;; DT[V2 %between% c(3, 5)]
;; DT[data.table::between(V2, 3, 5, incbounds = FALSE)]
;; DT[V2 %inrange% list(-1:1, 1:3)] # see also ?inrange

(r/bra DT '((rsymbol "%like%") V4 "^B"))
;; =>    V1 V2 V3 V4
;;    1:  2  2  1  B
;;    2:  1  5  1  B
;;    3:  2  8  1  B

(r/bra DT '((rsymbol "%between%") V2 [3 5]))
;; =>    V1 V2  V3 V4
;;    1:  1  3 1.5  C
;;    2:  2  4 0.5  A
;;    3:  1  5 1.0  B

(r/bra DT '((rsymbol data.table between) V2 3 5 :incbounds false))
;; =>    V1 V2  V3 V4
;;    1:  2  4 0.5  A

(r/bra DT '((rsymbol "%inrange%") V2 [:!list (colon -1 1) (colon 1 3)]))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C

;; ---- dplyr

;; filter(DF, grepl("^B", V4))
;; filter(DF, dplyr::between(V2, 3, 5))
;; filter(DF, V2 > 3 & V2 < 5)
;; filter(DF, V2 >= -1:1 & V2 <= 1:3)

(dpl/filter DF '(grepl "^B" V4))
;; => # A tibble: 3 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     2     2     1 B    
;;    2     1     5     1 B    
;;    3     2     8     1 B

(dpl/filter DF '((rsymbol dplyr between) V2 3 5))
;; => # A tibble: 3 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     3   1.5 C    
;;    2     2     4   0.5 A    
;;    3     1     5   1   B

(dpl/filter DF '(& (> V2 3) (< V2 5)))
;; => # A tibble: 1 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     2     4   0.5 A

(dpl/filter DF '(& (>= V2 (colon -1 1))
                   (<= V2 (colon 1 3))))
;; => # A tibble: 3 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     3   1.5 C    

;; ---- tech.ml.dataset

(ds/filter #(re-matches #"^B" (str (:V4 %))) DS)
;; => _unnamed [3 4]:
;;    | :V1 | :V2 |   :V3 | :V4 |
;;    |-----+-----+-------+-----|
;;    |   2 |   2 | 1.000 |   B |
;;    |   1 |   5 | 1.000 |   B |
;;    |   2 |   8 | 1.000 |   B |

(ds/filter (comp (partial m/between? 3 5) :V2) DS)
;; => _unnamed [3 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |

(ds/filter (comp #(< 3 % 5) :V2) DS)
;; => _unnamed [1 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   2 |   4 | 0.5000 |   A |

;; note: there is no range on sequences, I have no idea why to use such
(let [mn (min -1 0 1)
      mx (max 1 2 3)]
  (ds/filter (comp (partial m/between? mn mx) :V2) DS))
;; => _unnamed [3 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |

;; ## Sort rows

;; ### Sort rows by column

;; ---- data.table

;; DT[order(V3)]  # see also setorder

(r/bra DT '(order V3))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  4 0.5  A
;;    3:  1  7 0.5  A
;;    4:  2  2 1.0  B
;;    5:  1  5 1.0  B
;;    6:  2  8 1.0  B
;;    7:  1  3 1.5  C
;;    8:  2  6 1.5  C
;;    9:  1  9 1.5  C

;; ---- dplyr

;; arrange(DF, V3)

(dpl/arrange DF 'V3)
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     4   0.5 A    
;;    3     1     7   0.5 A    
;;    4     2     2   1   B    
;;    5     1     5   1   B    
;;    6     2     8   1   B    
;;    7     1     3   1.5 C    
;;    8     2     6   1.5 C    
;;    9     1     9   1.5 C    

;; ---- tech.ml.dataset

(ds/sort-by-column :V3 DS)
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   9 |  1.500 |   C |

;; alternative use of indices
(ds/select-rows DS (m/order (DS :V3)))
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   9 |  1.500 |   C |

;; ### Sort rows in decreasing order

;; ---- data.table

;; DT[order(-V3)]

(r/bra DT '(order (- V3)))
;; =>    V1 V2  V3 V4
;;    1:  1  3 1.5  C
;;    2:  2  6 1.5  C
;;    3:  1  9 1.5  C
;;    4:  2  2 1.0  B
;;    5:  1  5 1.0  B
;;    6:  2  8 1.0  B
;;    7:  1  1 0.5  A
;;    8:  2  4 0.5  A
;;    9:  1  7 0.5  A

;; ---- dplyr

;; arrange(DF, desc(V3))

(dpl/arrange DF '(desc V3))
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     3   1.5 C    
;;    2     2     6   1.5 C    
;;    3     1     9   1.5 C    
;;    4     2     2   1   B    
;;    5     1     5   1   B    
;;    6     2     8   1   B    
;;    7     1     1   0.5 A    
;;    8     2     4   0.5 A    
;;    9     1     7   0.5 A    

;; ---- tech.ml.dataset

(ds/sort-by-column :V3 (comp - compare) DS)
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   9 |  1.500 |   C |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   2 |  1.000 |   B |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   1 | 0.5000 |   A |

;; using ordering indices
(ds/select-rows DS (m/order (DS :V3) true))
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   9 |  1.500 |   C |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   7 | 0.5000 |   A |

;; ### Sort rows based on several columns

;; ---- data.table

;; DT[order(V1, -V2)]

(r/bra DT '(order V1 (- V2)))
;; =>    V1 V2  V3 V4
;;    1:  1  9 1.5  C
;;    2:  1  7 0.5  A
;;    3:  1  5 1.0  B
;;    4:  1  3 1.5  C
;;    5:  1  1 0.5  A
;;    6:  2  8 1.0  B
;;    7:  2  6 1.5  C
;;    8:  2  4 0.5  A
;;    9:  2  2 1.0  B

;; ---- dplyr

;; arrange(DF, V1, desc(V2))

(dpl/arrange DF 'V1 '(desc V2))
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     9   1.5 C    
;;    2     1     7   0.5 A    
;;    3     1     5   1   B    
;;    4     1     3   1.5 C    
;;    5     1     1   0.5 A    
;;    6     2     8   1   B    
;;    7     2     6   1.5 C    
;;    8     2     4   0.5 A    
;;    9     2     2   1   B    

;; ---- tech.ml.dataset

;; TODO: improve sorting in general
(sort-by-columns-with-orders [:V1 :V2] [:asc :desc] DS)
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   9 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |

;; ## Select columns

;; ### Select one column using an index (not recommended)

;; ---- data.table

;; DT[[3]] # returns a vector
;; DT[, 3]  # returns a data.table

(r/brabra DT 3)
;; => [1] 0.5 1.0 1.5 0.5 1.0 1.5 0.5 1.0 1.5

(r '(bra ~DT nil 3))
;; =>     V3
;;    1: 0.5
;;    2: 1.0
;;    3: 1.5
;;    4: 0.5
;;    5: 1.0
;;    6: 1.5
;;    7: 0.5
;;    8: 1.0
;;    9: 1.5

;; ---- dplyr

;; DF[[3]] # returns a vector
;; DF[3]   # returns a tibble

(r/brabra DF 3)
;; => [1] 0.5 1.0 1.5 0.5 1.0 1.5 0.5 1.0 1.5

(r/bra DF 3)
;; => # A tibble: 9 x 1
;;         V3
;;      <dbl>
;;    1   0.5
;;    2   1  
;;    3   1.5
;;    4   0.5
;;    5   1  
;;    6   1.5
;;    7   0.5
;;    8   1  
;;    9   1.5

;; ---- tech.ml.dataset

;; NOTE: indices start at 0
(nth (seq DS) 2)
;; => #tech.ml.dataset.column<float64>[9]
;;    :V3
;;    [0.5000, 1.000, 1.500, 0.5000, 1.000, 1.500, 0.5000, 1.000, 1.500, ]

;; NOTE: column is seqable
(seq (nth (seq DS) 2))
;; => (0.5 1.0 1.5 0.5 1.0 1.5 0.5 1.0 1.5)

(ds/new-dataset [(nth (seq DS) 2)])
;; => _unnamed [9 1]:
;;    |    :V3 |
;;    |--------|
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |

;; ### Select one column using column name

;; DT[, list(V2)] # returns a data.table
;; DT[, .(V2)]    # returns a data.table
;; # . is an alias for list
;; DT[, "V2"]     # returns a data.table
;; DT[, V2]       # returns a vector
;; DT[["V2"]]     # returns a vector

;; ---- data.table

(r '(bra ~DT nil [:!list V2]))
;; =>    V2
;;    1:  1
;;    2:  2
;;    3:  3
;;    4:  4
;;    5:  5
;;    6:  6
;;    7:  7
;;    8:  8
;;    9:  9

(r '(bra ~DT nil (. V2)))
;; =>    V2
;;    1:  1
;;    2:  2
;;    3:  3
;;    4:  4
;;    5:  5
;;    6:  6
;;    7:  7
;;    8:  8
;;    9:  9

(r '(bra ~DT nil "V2"))
;; =>    V2
;;    1:  1
;;    2:  2
;;    3:  3
;;    4:  4
;;    5:  5
;;    6:  6
;;    7:  7
;;    8:  8
;;    9:  9

(r '(bra ~DT nil V2))
;; => [1] 1 2 3 4 5 6 7 8 9

(r/brabra DT "V2")
;; => [1] 1 2 3 4 5 6 7 8 9

;; ---- dplyr

;; select(DF, V2) # returns a tibble
;; pull(DF, V2)   # returns a vector
;; DF[, "V2"]        # returns a tibble
;; DF[["V2"]]        # returns a vector

(dpl/select DF 'V2)
;; => # A tibble: 9 x 1
;;         V2
;;      <int>
;;    1     1
;;    2     2
;;    3     3
;;    4     4
;;    5     5
;;    6     6
;;    7     7
;;    8     8
;;    9     9

(dpl/pull DF 'V2)
;; => [1] 1 2 3 4 5 6 7 8 9

(r '(bra ~DF nil "V2"))
;; => # A tibble: 9 x 1
;;         V2
;;      <int>
;;    1     1
;;    2     2
;;    3     3
;;    4     4
;;    5     5
;;    6     6
;;    7     7
;;    8     8
;;    9     9

(r/brabra DF "V2")
;; => [1] 1 2 3 4 5 6 7 8 9

;; ---- tech.ml.dataset

;; NOTE: returns dataset
(ds/select-columns DS [:V2])
;; => _unnamed [9 1]:
;;    | :V2 |
;;    |-----|
;;    |   1 |
;;    |   2 |
;;    |   3 |
;;    |   4 |
;;    |   5 |
;;    |   6 |
;;    |   7 |
;;    |   8 |
;;    |   9 |

;; NOTE: returns dataset
(ds/select DS [:V2] :all)
;; => _unnamed [9 1]:
;;    | :V2 |
;;    |-----|
;;    |   1 |
;;    |   2 |
;;    |   3 |
;;    |   4 |
;;    |   5 |
;;    |   6 |
;;    |   7 |
;;    |   8 |
;;    |   9 |

;; NOTE: returns column (seqable)
(DS :V2)
;; => #tech.ml.dataset.column<int64>[9]
;;    :V2
;;    [1, 2, 3, 4, 5, 6, 7, 8, 9, ]

;; NOTE: column as sequence
(seq (DS :V2))
;; => (1 2 3 4 5 6 7 8 9)

;; NOTE: efficient access to data via reader
(dtype/->reader (DS :V2))
;; => [1 2 3 4 5 6 7 8 9]

;; NOTE: returns column
(ds/column DS :V2)
;; => #tech.ml.dataset.column<int64>[9]
;;    :V2
;;    [1, 2, 3, 4, 5, 6, 7, 8, 9, ]

;; ### Select several columns

;; ---- data.table

;; DT[, .(V2, V3, V4)]
;; DT[, list(V2, V3, V4)]
;; DT[, V2:V4] # select columns between V2 and V4

(r '(bra ~DT nil (. V2 V3 V4)))
;; =>    V2  V3 V4
;;    1:  1 0.5  A
;;    2:  2 1.0  B
;;    3:  3 1.5  C
;;    4:  4 0.5  A
;;    5:  5 1.0  B
;;    6:  6 1.5  C
;;    7:  7 0.5  A
;;    8:  8 1.0  B
;;    9:  9 1.5  C

(r '(bra ~DT nil (:!list V2 V3 V4)))
;; =>    V2  V3 V4
;;    1:  1 0.5  A
;;    2:  2 1.0  B
;;    3:  3 1.5  C
;;    4:  4 0.5  A
;;    5:  5 1.0  B
;;    6:  6 1.5  C
;;    7:  7 0.5  A
;;    8:  8 1.0  B
;;    9:  9 1.5  C

(r '(bra ~DT nil (colon V2 V4)))
;; =>    V2  V3 V4
;;    1:  1 0.5  A
;;    2:  2 1.0  B
;;    3:  3 1.5  C
;;    4:  4 0.5  A
;;    5:  5 1.0  B
;;    6:  6 1.5  C
;;    7:  7 0.5  A
;;    8:  8 1.0  B
;;    9:  9 1.5  C

;; ---- dplyr

;; select(DF, V2, V3, V4)
;; select(DF, V2:V4) # select columns between V2 and V4

(dpl/select DF 'V2 'V3 'V4)
;; => # A tibble: 9 x 3
;;         V2    V3 V4   
;;      <int> <dbl> <chr>
;;    1     1   0.5 A    
;;    2     2   1   B    
;;    3     3   1.5 C    
;;    4     4   0.5 A    
;;    5     5   1   B    
;;    6     6   1.5 C    
;;    7     7   0.5 A    
;;    8     8   1   B    
;;    9     9   1.5 C

(dpl/select DF '(colon V2 V4))
;; => # A tibble: 9 x 3
;;         V2    V3 V4   
;;      <int> <dbl> <chr>
;;    1     1   0.5 A    
;;    2     2   1   B    
;;    3     3   1.5 C    
;;    4     4   0.5 A    
;;    5     5   1   B    
;;    6     6   1.5 C    
;;    7     7   0.5 A    
;;    8     8   1   B    
;;    9     9   1.5 C

;; ---- tech.ml.dataset

(ds/select-columns DS [:V2 :V3 :V4])
;; => _unnamed [9 3]:
;;    | :V2 |    :V3 | :V4 |
;;    |-----+--------+-----|
;;    |   1 | 0.5000 |   A |
;;    |   2 |  1.000 |   B |
;;    |   3 |  1.500 |   C |
;;    |   4 | 0.5000 |   A |
;;    |   5 |  1.000 |   B |
;;    |   6 |  1.500 |   C |
;;    |   7 | 0.5000 |   A |
;;    |   8 |  1.000 |   B |
;;    |   9 |  1.500 |   C |

(ds/select DS [:V2 :V3 :V4] :all)
;; => _unnamed [9 3]:
;;    | :V2 |    :V3 | :V4 |
;;    |-----+--------+-----|
;;    |   1 | 0.5000 |   A |
;;    |   2 |  1.000 |   B |
;;    |   3 |  1.500 |   C |
;;    |   4 | 0.5000 |   A |
;;    |   5 |  1.000 |   B |
;;    |   6 |  1.500 |   C |
;;    |   7 | 0.5000 |   A |
;;    |   8 |  1.000 |   B |
;;    |   9 |  1.500 |   C |

;; NOTE: range is not available

;; ### Exclude columns

;; ---- data.table

;; DT[, !c("V2", "V3")]

(r '(bra ~DT nil (! ["V2" "V3"])))
;; =>    V1 V4
;;    1:  1  A
;;    2:  2  B
;;    3:  1  C
;;    4:  2  A
;;    5:  1  B
;;    6:  2  C
;;    7:  1  A
;;    8:  2  B
;;    9:  1  C

;; ---- dplyr

;; select(DF, -V2, -V3)

(dpl/select DF '(- V2) '(- V3))
;; => # A tibble: 9 x 2
;;         V1 V4   
;;      <dbl> <chr>
;;    1     1 A    
;;    2     2 B    
;;    3     1 C    
;;    4     2 A    
;;    5     1 B    
;;    6     2 C    
;;    7     1 A    
;;    8     2 B    
;;    9     1 C    

;; ---- tech.ml.dataset

(ds/drop-columns DS [:V2 :V3])
;; => _unnamed [9 2]:
;;    | :V1 | :V4 |
;;    |-----+-----|
;;    |   1 |   A |
;;    |   2 |   B |
;;    |   1 |   C |
;;    |   2 |   A |
;;    |   1 |   B |
;;    |   2 |   C |
;;    |   1 |   A |
;;    |   2 |   B |
;;    |   1 |   C |

;; ### Select/Exclude columns using a character vector

;; ---- data.table

;; cols <- c("V2", "V3")
;; DT[, ..cols] # .. prefix means 'one-level up'
;; DT[, !..cols] # or DT[, -..cols]

(def cols (r.base/<- 'cols ["V2" "V3"]))

(r '(bra ~DT nil ..cols))
;; =>    V2  V3
;;    1:  1 0.5
;;    2:  2 1.0
;;    3:  3 1.5
;;    4:  4 0.5
;;    5:  5 1.0
;;    6:  6 1.5
;;    7:  7 0.5
;;    8:  8 1.0
;;    9:  9 1.5

(r '(bra ~DT nil !..cols))
;; =>    V1 V4
;;    1:  1  A
;;    2:  2  B
;;    3:  1  C
;;    4:  2  A
;;    5:  1  B
;;    6:  2  C
;;    7:  1  A
;;    8:  2  B
;;    9:  1  C

(r '(bra ~DT nil -..cols))
;; =>    V1 V4
;;    1:  1  A
;;    2:  2  B
;;    3:  1  C
;;    4:  2  A
;;    5:  1  B
;;    6:  2  C
;;    7:  1  A
;;    8:  2  B
;;    9:  1  C

;; ---- dplyr

;; cols <- c("V2", "V3")
;; select(DF, !!cols) # unquoting
;; select(DF, -!!cols)

(def cols (r.base/<- 'cols ["V2" "V3"]))

(dpl/select DF '!!cols)
;; => # A tibble: 9 x 2
;;         V2    V3
;;      <int> <dbl>
;;    1     1   0.5
;;    2     2   1  
;;    3     3   1.5
;;    4     4   0.5
;;    5     5   1  
;;    6     6   1.5
;;    7     7   0.5
;;    8     8   1  
;;    9     9   1.5

(dpl/select DF '-!!cols)
;; => # A tibble: 9 x 2
;;         V1 V4   
;;      <dbl> <chr>
;;    1     1 A    
;;    2     2 B    
;;    3     1 C    
;;    4     2 A    
;;    5     1 B    
;;    6     2 C    
;;    7     1 A    
;;    8     2 B    
;;    9     1 C    

;; ---- tech.ml.dataset

(def cols [:V2 :V3])

(ds/select-columns DS cols)
;; => _unnamed [9 2]:
;;    | :V2 |    :V3 |
;;    |-----+--------|
;;    |   1 | 0.5000 |
;;    |   2 |  1.000 |
;;    |   3 |  1.500 |
;;    |   4 | 0.5000 |
;;    |   5 |  1.000 |
;;    |   6 |  1.500 |
;;    |   7 | 0.5000 |
;;    |   8 |  1.000 |
;;    |   9 |  1.500 |

(ds/drop-columns DS cols)
;; => _unnamed [9 2]:
;;    | :V1 | :V4 |
;;    |-----+-----|
;;    |   1 |   A |
;;    |   2 |   B |
;;    |   1 |   C |
;;    |   2 |   A |
;;    |   1 |   B |
;;    |   2 |   C |
;;    |   1 |   A |
;;    |   2 |   B |
;;    |   1 |   C |

;; ### Other selections

;; ---- data.table

;; cols <- paste0("V", 1:2)
;; cols <- union("V4", names(DT))
;; cols <- grep("V",   names(DT))
;; cols <- grep("3$",  names(DT))
;; cols <- grep(".2",  names(DT))
;; cols <- grep("^V1|X$",  names(DT))
;; cols <- grep("^(?!V2)", names(DT), perl = TRUE)
;; DT[, ..cols]

(r.base/<- 'cols (r.base/paste0 "V" (r/colon 1 2)))
;; => [1] "V1" "V2"
(r '(bra ~DT nil ..cols))
;; =>    V1 V2
;;    1:  1  1
;;    2:  2  2
;;    3:  1  3
;;    4:  2  4
;;    5:  1  5
;;    6:  2  6
;;    7:  1  7
;;    8:  2  8
;;    9:  1  9

(r.base/<- 'cols (r.base/union "V4" (r.base/names DT)))
;; => [1] "V4" "V1" "V2" "V3"
(r '(bra ~DT nil ..cols))
;; =>    V4 V1 V2  V3
;;    1:  A  1  1 0.5
;;    2:  B  2  2 1.0
;;    3:  C  1  3 1.5
;;    4:  A  2  4 0.5
;;    5:  B  1  5 1.0
;;    6:  C  2  6 1.5
;;    7:  A  1  7 0.5
;;    8:  B  2  8 1.0
;;    9:  C  1  9 1.5

(r.base/<- 'cols (r.base/grep "V" (r.base/names DT)))
;; => [1] 1 2 3 4
(r '(bra ~DT nil ..cols))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  2  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  2  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  2  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  2  8 1.0  B
;;    9:  1  9 1.5  C

(r.base/<- 'cols (r.base/grep "3$" (r.base/names DT)))
;; => [1] 3
(r '(bra ~DT nil ..cols))
;; =>     V3
;;    1: 0.5
;;    2: 1.0
;;    3: 1.5
;;    4: 0.5
;;    5: 1.0
;;    6: 1.5
;;    7: 0.5
;;    8: 1.0
;;    9: 1.5

(r.base/<- 'cols (r.base/grep ".2" (r.base/names DT)))
;; => [1] 2
(r '(bra ~DT nil ..cols))
;; =>    V2
;;    1:  1
;;    2:  2
;;    3:  3
;;    4:  4
;;    5:  5
;;    6:  6
;;    7:  7
;;    8:  8
;;    9:  9

(r.base/<- 'cols (r.base/grep "^V1|X$" (r.base/names DT)))
;; => [1] 1
(r '(bra ~DT nil ..cols))
;; =>    V1
;;    1:  1
;;    2:  2
;;    3:  1
;;    4:  2
;;    5:  1
;;    6:  2
;;    7:  1
;;    8:  2
;;    9:  1

(r.base/<- 'cols (r.base/grep "^(?!V2)" (r.base/names DT) :perl true))
;; => [1] 1 3 4
(r '(bra ~DT nil ..cols))
;; =>    V1  V3 V4
;;    1:  1 0.5  A
;;    2:  2 1.0  B
;;    3:  1 1.5  C
;;    4:  2 0.5  A
;;    5:  1 1.0  B
;;    6:  2 1.5  C
;;    7:  1 0.5  A
;;    8:  2 1.0  B
;;    9:  1 1.5  C

;; ---- dplyr

;; select(DF, num_range("V", 1:2))
;; select(DF, V4, everything()) # reorder columns
;; select(DF, contains("V"))
;; select(DF, ends_with("3"))
;; select(DF, matches(".2"))
;; select(DF, one_of(c("V1", "X")))
;; select(DF, -starts_with("V2"))

(dpl/select DF '(num_range "V" (colon 1 2)))
;; => # A tibble: 9 x 2
;;         V1    V2
;;      <dbl> <int>
;;    1     1     1
;;    2     2     2
;;    3     1     3
;;    4     2     4
;;    5     1     5
;;    6     2     6
;;    7     1     7
;;    8     2     8
;;    9     1     9

(dpl/select DF 'V4 '(everything))
;; => # A tibble: 9 x 4
;;      V4       V1    V2    V3
;;      <chr> <dbl> <int> <dbl>
;;    1 A         1     1   0.5
;;    2 B         2     2   1  
;;    3 C         1     3   1.5
;;    4 A         2     4   0.5
;;    5 B         1     5   1  
;;    6 C         2     6   1.5
;;    7 A         1     7   0.5
;;    8 B         2     8   1  
;;    9 C         1     9   1.5

(dpl/select DF '(contains "V"))
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     2     2   1   B    
;;    3     1     3   1.5 C    
;;    4     2     4   0.5 A    
;;    5     1     5   1   B    
;;    6     2     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     2     8   1   B    
;;    9     1     9   1.5 C

(dpl/select DF '(ends_with "3"))
;; => # A tibble: 9 x 1
;;         V3
;;      <dbl>
;;    1   0.5
;;    2   1  
;;    3   1.5
;;    4   0.5
;;    5   1  
;;    6   1.5
;;    7   0.5
;;    8   1  
;;    9   1.5

(dpl/select DF '(matches ".2"))
;; => # A tibble: 9 x 1
;;         V2
;;      <int>
;;    1     1
;;    2     2
;;    3     3
;;    4     4
;;    5     5
;;    6     6
;;    7     7
;;    8     8
;;    9     9

(dpl/select DF '(one_of ["V1" "X"]))
;; => # A tibble: 9 x 1
;;         V1
;;      <dbl>
;;    1     1
;;    2     2
;;    3     1
;;    4     2
;;    5     1
;;    6     2
;;    7     1
;;    8     2
;;    9     1

(dpl/select DF '(- (starts_with "V2")))
;; => # A tibble: 9 x 3
;;         V1    V3 V4   
;;      <dbl> <dbl> <chr>
;;    1     1   0.5 A    
;;    2     2   1   B    
;;    3     1   1.5 C    
;;    4     2   0.5 A    
;;    5     1   1   B    
;;    6     2   1.5 C    
;;    7     1   0.5 A    
;;    8     2   1   B    
;;    9     1   1.5 C    

;; ---- tech.ml.dataset

;; NOTE: we are using pure Clojure machinery to generate column names

(->> (map (comp keyword str) (repeat "V") (range 1 3))
     (ds/select-columns DS))
;; => _unnamed [9 2]:
;;    | :V1 | :V2 |
;;    |-----+-----|
;;    |   1 |   1 |
;;    |   2 |   2 |
;;    |   1 |   3 |
;;    |   2 |   4 |
;;    |   1 |   5 |
;;    |   2 |   6 |
;;    |   1 |   7 |
;;    |   2 |   8 |
;;    |   1 |   9 |

(->> (distinct (conj (ds/column-names DS) :V4))
     (ds/select-columns DS))
;; => _unnamed [9 4]:
;;    | :V4 | :V1 | :V2 |    :V3 |
;;    |-----+-----+-----+--------|
;;    |   A |   1 |   1 | 0.5000 |
;;    |   B |   2 |   2 |  1.000 |
;;    |   C |   1 |   3 |  1.500 |
;;    |   A |   2 |   4 | 0.5000 |
;;    |   B |   1 |   5 |  1.000 |
;;    |   C |   2 |   6 |  1.500 |
;;    |   A |   1 |   7 | 0.5000 |
;;    |   B |   2 |   8 |  1.000 |
;;    |   C |   1 |   9 |  1.500 |

(->> (ds/column-names DS)
     (filter #(str/starts-with? (name %) "V"))
     (ds/select-columns DS))
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   1 |   1 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   9 |  1.500 |   C |

(->> (ds/column-names DS)
     (filter #(str/ends-with? (name %) "3"))
     (ds/select-columns DS))
;; => _unnamed [9 1]:
;;    |    :V3 |
;;    |--------|
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |
;;    | 0.5000 |
;;    |  1.000 |
;;    |  1.500 |

(->> (ds/column-names DS)
     (filter #(re-matches #".2" (name %)))
     (ds/select-columns DS))
;; => _unnamed [9 1]:
;;    | :V2 |
;;    |-----|
;;    |   1 |
;;    |   2 |
;;    |   3 |
;;    |   4 |
;;    |   5 |
;;    |   6 |
;;    |   7 |
;;    |   8 |
;;    |   9 |

(->> (ds/column-names DS)
     (filter #{:V1 :X})
     (ds/select-columns DS))
;; => _unnamed [9 1]:
;;    | :V1 |
;;    |-----|
;;    |   1 |
;;    |   2 |
;;    |   1 |
;;    |   2 |
;;    |   1 |
;;    |   2 |
;;    |   1 |
;;    |   2 |
;;    |   1 |

(->> (ds/column-names DS)
     (remove #(str/starts-with? (name %) "V2"))
     (ds/select-columns DS))
;; => _unnamed [9 3]:
;;    | :V1 |    :V3 | :V4 |
;;    |-----+--------+-----|
;;    |   1 | 0.5000 |   A |
;;    |   2 |  1.000 |   B |
;;    |   1 |  1.500 |   C |
;;    |   2 | 0.5000 |   A |
;;    |   1 |  1.000 |   B |
;;    |   2 |  1.500 |   C |
;;    |   1 | 0.5000 |   A |
;;    |   2 |  1.000 |   B |
;;    |   1 |  1.500 |   C |

;; ## Summarise data

;; ### Summarise one column

;; ---- data.table

;; DT[, sum(V1)]    # returns a vector
;; DT[, .(sum(V1))] # returns a data.table
;; DT[, .(sumV1 = sum(V1))] # returns a data.table

(r '(bra ~DT nil (sum V1)))
;; => [1] 13

(r '(bra ~DT nil (. (sum V1))))
;; =>    V1
;;    1: 13

(r '(bra ~DT nil (. :sumV1 (sum V1))))
;; =>    sumV1
;;    1:    13

;; ---- dplyr

;; summarise(DF, sum(V1)) # returns a tibble
;; summarise(DF, sumV1 = sum(V1)) # returns a tibble

(dpl/summarise DF '(sum V1))
;; => # A tibble: 1 x 1
;;      `sum(V1)`
;;          <dbl>
;;    1        13

(dpl/summarise DF :sumV1 '(sum V1))
;; => # A tibble: 1 x 1
;;      sumV1
;;      <dbl>
;;    1    13

;; ---- tech.ml.dataset

;; NOTE: using optimized datatype function 
(dfn/sum (DS :V1))
;; => 13.0

;; NOTE: using reduce
(reduce + (DS :V1))
;; => 13

;; NOTE: custom aggregation function to get back dataset (issue filled)
;; TODO: aggregate->dataset
(aggregate->dataset [#(dfn/sum (% :V1))] DS)
;; => _unnamed [1 1]:
;;    |     0 |
;;    |-------|
;;    | 13.00 |

(aggregate->dataset {:sumV1 #(dfn/sum (% :V1))} DS)
;; => _unnamed [1 1]:
;;    | :sumV1 |
;;    |--------|
;;    |  13.00 |

;; ### Summarise several columns

;; ---- data.table

;; DT[, .(sum(V1), sd(V3))]

(r '(bra ~DT nil (. (sum V1) (sd V3))))
;; =>    V1        V2
;;    1: 13 0.4330127

;; ---- dplyr

;; summarise(DF, sum(V1), sd(V3))

(dpl/summarise DF '(sum V1) '(sd V3))
;; => # A tibble: 1 x 2
;;      `sum(V1)` `sd(V3)`
;;          <dbl>    <dbl>
;;    1        13    0.433

;; ---- tech.ml.dataset

(aggregate->dataset [#(dfn/sum (% :V1))
                     #(dfn/standard-deviation (% :V3))] DS)
;; => _unnamed [1 2]:
;;    |     0 |      1 |
;;    |-------+--------|
;;    | 13.00 | 0.4330 |

;; ### Summarise several columns and assign column names

;; ---- data.table

;; DT[, .(sumv1 = sum(V1),
;;        sdv3  = sd(V3))]

(r '(bra ~DT nil (. :sumv1 (sum V1) :sdv3 (sd V3))))
;; =>    sumv1      sdv3
;;    1:    13 0.4330127

;; ---- dplyr

;; DF %>%
;;   summarise(sumv1 = sum(V1),
;;             sdv3  = sd(V3))

(-> DF
    (dpl/summarise :sumv1 '(sum V1)
                   :sdv3 '(sd V3)))
;; => # A tibble: 1 x 2
;;      sumv1  sdv3
;;      <dbl> <dbl>
;;    1    13 0.433

;; ---- tech.ml.dataset

(aggregate->dataset {:sumv1 #(dfn/sum (% :V1))
                     :sdv3 #(dfn/standard-deviation (% :V3))} DS)
;; => _unnamed [1 2]:
;;    | :sumv1 |  :sdv3 |
;;    |--------+--------|
;;    |  13.00 | 0.4330 |

;; ### Summarise a subset of rows

;; ---- data.table

;; DT[1:4, sum(V1)]

(r/bra DT (r/colon 1 4) '(sum V1))
;; => [1] 6

;; ---- dplyr

;; DF %>%
;;   slice(1:4) %>%
;;   summarise(sum(V1))

(-> DF
    (dpl/slice (r/colon 1 4))
    (dpl/summarise '(sum V1)))
;; => # A tibble: 1 x 1
;;      `sum(V1)`
;;          <dbl>
;;    1         6

;; ---- tech.ml.dataset

(-> DS
    (ds/select-rows (range 4))
    (->> (aggregate->dataset [#(dfn/sum (% :V1))])))
;; => _unnamed [1 1]:
;;    |     0 |
;;    |-------|
;;    | 6.000 |

;; ### additional

;; ---- data.table

;; DT[, data.table::first(V3)]
;; DT[, data.table::last(V3)]
;; DT[5, V3]
;; DT[, uniqueN(V4)]
;; uniqueN(DT)

(r '(bra ~DT nil ((rsymbol data.table first) V3)))
;; => [1] 0.5

(r '(bra ~DT nil ((rsymbol data.table last) V3)))
;; => [1] 1.5

(r/bra DT 5 'V3)
;; => [1] 1

(r '(bra ~DT nil (uniqueN V4)))
;; => [1] 3

(dt/uniqueN DT)
;; => [1] 9

;; ---- dplyr

;; summarise(DF, dplyr::first(V3))
;; summarise(DF, dplyr::last(V3))
;; summarise(DF, nth(V3, 5))
;; summarise(DF, n_distinct(V4))
;; n_distinct(DF)

(dpl/summarise DF '((rsymbol dplyr first) V3))
;; => # A tibble: 1 x 1
;;      `dplyr::first(V3)`
;;                   <dbl>
;;    1                0.5

(dpl/summarise DF '((rsymbol dplyr last) V3))
;; => # A tibble: 1 x 1
;;      `dplyr::last(V3)`
;;                  <dbl>
;;    1               1.5

(dpl/summarise DF '(nth V3 5))
;; => # A tibble: 1 x 1
;;      `nth(V3, 5)`
;;             <dbl>
;;    1            1

(dpl/summarise DF '(n_distinct V4))
;; => # A tibble: 1 x 1
;;      `n_distinct(V4)`
;;                 <int>
;;    1                3

(dpl/n_distinct DF)
;; => [1] 9

;; ---- tech.ml.dataset

(first (DS :V3))
;; => 0.5

(last (DS :V3))
;; => 1.5

(nth (DS :V3) 5)
;; => 1.5

(count (col/unique (DS :V3)))
;; => 3

(ds/row-count (ds/unique-by identity DS))
;; => 9

;; ## Add/update/delete columns

;; ### Modify a column

;; ---- data.table

;; DT[, V1 := V1^2]
;; DT

;; TODO (clojisr): another tricky binary operator
(r '(bra ~DT nil ((rsymbol ":=") V1 (** V1 2))))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  4  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  4  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  4  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  4  8 1.0  B
;;    9:  1  9 1.5  C

;; ---- dplyr

;; DF <- DF %>% mutate(V1 = V1^2)
;; DF

(def DF (-> DF (dpl/mutate :V1 '(** V1 2))))
DF
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     4     2   1   B    
;;    3     1     3   1.5 C    
;;    4     4     4   0.5 A    
;;    5     1     5   1   B    
;;    6     4     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     4     8   1   B    
;;    9     1     9   1.5 C

;; ---- tech.ml.dataset

(ds/update-column DS :V1 #(map (fn [v] (m/pow v 2)) %))
;; => _unnamed [9 4]:
;;    |   :V1 | :V2 |    :V3 | :V4 |
;;    |-------+-----+--------+-----|
;;    | 1.000 |   1 | 0.5000 |   A |
;;    | 4.000 |   2 |  1.000 |   B |
;;    | 1.000 |   3 |  1.500 |   C |
;;    | 4.000 |   4 | 0.5000 |   A |
;;    | 1.000 |   5 |  1.000 |   B |
;;    | 4.000 |   6 |  1.500 |   C |
;;    | 1.000 |   7 | 0.5000 |   A |
;;    | 4.000 |   8 |  1.000 |   B |
;;    | 1.000 |   9 |  1.500 |   C |

;; NOTE: using reader (optimized)
(def DS (ds/update-column DS :V1 #(-> (dtype/->reader % :float64)
                                      (dfn/pow 2))))
DS
;; => _unnamed [9 4]:
;;    |   :V1 | :V2 |    :V3 | :V4 |
;;    |-------+-----+--------+-----|
;;    | 1.000 |   1 | 0.5000 |   A |
;;    | 4.000 |   2 |  1.000 |   B |
;;    | 1.000 |   3 |  1.500 |   C |
;;    | 4.000 |   4 | 0.5000 |   A |
;;    | 1.000 |   5 |  1.000 |   B |
;;    | 4.000 |   6 |  1.500 |   C |
;;    | 1.000 |   7 | 0.5000 |   A |
;;    | 4.000 |   8 |  1.000 |   B |
;;    | 1.000 |   9 |  1.500 |   C |

;; ### Add one column

;; ---- data.table

;; DT[, v5 := log(V1)][] # adding [] prints the result

(r '(bra (bra ~DT nil ((rsymbol ":=") v5 (log V1)))))
;; =>    V1 V2  V3 V4       v5
;;    1:  1  1 0.5  A 0.000000
;;    2:  4  2 1.0  B 1.386294
;;    3:  1  3 1.5  C 0.000000
;;    4:  4  4 0.5  A 1.386294
;;    5:  1  5 1.0  B 0.000000
;;    6:  4  6 1.5  C 1.386294
;;    7:  1  7 0.5  A 0.000000
;;    8:  4  8 1.0  B 1.386294
;;    9:  1  9 1.5  C 0.000000

;; ---- dplyr

(def DF (dpl/mutate DF :v5 '(log V1)))
DF
;; => # A tibble: 9 x 5
;;         V1    V2    V3 V4       v5
;;      <dbl> <int> <dbl> <chr> <dbl>
;;    1     1     1   0.5 A      0   
;;    2     4     2   1   B      1.39
;;    3     1     3   1.5 C      0   
;;    4     4     4   0.5 A      1.39
;;    5     1     5   1   B      0   
;;    6     4     6   1.5 C      1.39
;;    7     1     7   0.5 A      0   
;;    8     4     8   1   B      1.39
;;    9     1     9   1.5 C      0   

;; ---- tech.ml.dataset

(def DS (ds/add-or-update-column DS :v5 (dfn/log (DS :V1))))
DS
;; => _unnamed [9 5]:
;;    |   :V1 | :V2 |    :V3 | :V4 |   :v5 |
;;    |-------+-----+--------+-----+-------|
;;    | 1.000 |   1 | 0.5000 |   A | 0.000 |
;;    | 4.000 |   2 |  1.000 |   B | 1.386 |
;;    | 1.000 |   3 |  1.500 |   C | 0.000 |
;;    | 4.000 |   4 | 0.5000 |   A | 1.386 |
;;    | 1.000 |   5 |  1.000 |   B | 0.000 |
;;    | 4.000 |   6 |  1.500 |   C | 1.386 |
;;    | 1.000 |   7 | 0.5000 |   A | 0.000 |
;;    | 4.000 |   8 |  1.000 |   B | 1.386 |
;;    | 1.000 |   9 |  1.500 |   C | 0.000 |

;; ### Add several columns

;; ---- data.table

;; DT[, c("v6", "v7") := .(sqrt(V1), "X")]
;; DT[, ':='(v6 = sqrt(V1),
;;           v7 = "X")]     # same, functional form

(r '(bra ~DT nil ((rsymbol ":=") ["v6" "v7"] (. (sqrt V1) "X"))))
;; =>    V1 V2  V3 V4       v5 v6 v7
;;    1:  1  1 0.5  A 0.000000  1  X
;;    2:  4  2 1.0  B 1.386294  2  X
;;    3:  1  3 1.5  C 0.000000  1  X
;;    4:  4  4 0.5  A 1.386294  2  X
;;    5:  1  5 1.0  B 0.000000  1  X
;;    6:  4  6 1.5  C 1.386294  2  X
;;    7:  1  7 0.5  A 0.000000  1  X
;;    8:  4  8 1.0  B 1.386294  2  X
;;    9:  1  9 1.5  C 0.000000  1  X

(r '(bra ~DT nil ((rsymbol ":=") :v6 (sqrt V1) :v7 "X")))
;; =>    V1 V2  V3 V4       v5 v6 v7
;;    1:  1  1 0.5  A 0.000000  1  X
;;    2:  4  2 1.0  B 1.386294  2  X
;;    3:  1  3 1.5  C 0.000000  1  X
;;    4:  4  4 0.5  A 1.386294  2  X
;;    5:  1  5 1.0  B 0.000000  1  X
;;    6:  4  6 1.5  C 1.386294  2  X
;;    7:  1  7 0.5  A 0.000000  1  X
;;    8:  4  8 1.0  B 1.386294  2  X
;;    9:  1  9 1.5  C 0.000000  1  X

;; ---- dplyr

;; DF <- mutate(DF, v6 = sqrt(V1), v7 = "X")

(def DF (dpl/mutate DF :v6 '(sqrt V1) :v7 "X"))
DF
;; => # A tibble: 9 x 7
;;         V1    V2    V3 V4       v5    v6 v7   
;;      <dbl> <int> <dbl> <chr> <dbl> <dbl> <chr>
;;    1     1     1   0.5 A      0        1 X    
;;    2     4     2   1   B      1.39     2 X    
;;    3     1     3   1.5 C      0        1 X    
;;    4     4     4   0.5 A      1.39     2 X    
;;    5     1     5   1   B      0        1 X    
;;    6     4     6   1.5 C      1.39     2 X    
;;    7     1     7   0.5 A      0        1 X    
;;    8     4     8   1   B      1.39     2 X    
;;    9     1     9   1.5 C      0        1 X    

;; ---- tech.ml.dataset

(def DS (-> DS
            (ds/add-or-update-column :v6 (dfn/sqrt (DS :V1)))
            (ds/add-or-update-column :v7 (take (ds/row-count DS) (repeat "X")))))
DS
;; => _unnamed [9 7]:
;;    |   :V1 | :V2 |    :V3 | :V4 |   :v5 |   :v6 | :v7 |
;;    |-------+-----+--------+-----+-------+-------+-----|
;;    | 1.000 |   1 | 0.5000 |   A | 0.000 | 1.000 |   X |
;;    | 4.000 |   2 |  1.000 |   B | 1.386 | 2.000 |   X |
;;    | 1.000 |   3 |  1.500 |   C | 0.000 | 1.000 |   X |
;;    | 4.000 |   4 | 0.5000 |   A | 1.386 | 2.000 |   X |
;;    | 1.000 |   5 |  1.000 |   B | 0.000 | 1.000 |   X |
;;    | 4.000 |   6 |  1.500 |   C | 1.386 | 2.000 |   X |
;;    | 1.000 |   7 | 0.5000 |   A | 0.000 | 1.000 |   X |
;;    | 4.000 |   8 |  1.000 |   B | 1.386 | 2.000 |   X |
;;    | 1.000 |   9 |  1.500 |   C | 0.000 | 1.000 |   X |

;; ### Create one column and remove the others

;; ---- data.table

;; DT[, .(v8 = V3 + 1)]

(r '(bra ~DT nil (. :v8 (+ V3 1))))
;; =>     v8
;;    1: 1.5
;;    2: 2.0
;;    3: 2.5
;;    4: 1.5
;;    5: 2.0
;;    6: 2.5
;;    7: 1.5
;;    8: 2.0
;;    9: 2.5

;; ---- dplyr

;; transmute(DF, v8 = V3 + 1)

(dpl/transmute DF :v8 '(+ V3 1))
;; => # A tibble: 9 x 1
;;         v8
;;      <dbl>
;;    1   1.5
;;    2   2  
;;    3   2.5
;;    4   1.5
;;    5   2  
;;    6   2.5
;;    7   1.5
;;    8   2  
;;    9   2.5

;; ---- tech.ml.dataset

(ds/new-dataset [(col/new-column :v8 (dfn/+ (DS :V3) 1))])
;; => _unnamed [9 1]:
;;    |   :v8 |
;;    |-------|
;;    | 1.500 |
;;    | 2.000 |
;;    | 2.500 |
;;    | 1.500 |
;;    | 2.000 |
;;    | 2.500 |
;;    | 1.500 |
;;    | 2.000 |
;;    | 2.500 |

;; ### Remove one column

;; ---- data.table

;; DT[, v5 := NULL]

(r '(bra ~DT nil ((rsymbol ":=") v5 nil)))
;; =>    V1 V2  V3 V4 v6 v7
;;    1:  1  1 0.5  A  1  X
;;    2:  4  2 1.0  B  2  X
;;    3:  1  3 1.5  C  1  X
;;    4:  4  4 0.5  A  2  X
;;    5:  1  5 1.0  B  1  X
;;    6:  4  6 1.5  C  2  X
;;    7:  1  7 0.5  A  1  X
;;    8:  4  8 1.0  B  2  X
;;    9:  1  9 1.5  C  1  X

;; ---- dplyr

;; DF <- select(DF, -v5)

(def DF (dpl/select DF '(- v5)))
DF
;; => # A tibble: 9 x 6
;;         V1    V2    V3 V4       v6 v7   
;;      <dbl> <int> <dbl> <chr> <dbl> <chr>
;;    1     1     1   0.5 A         1 X    
;;    2     4     2   1   B         2 X    
;;    3     1     3   1.5 C         1 X    
;;    4     4     4   0.5 A         2 X    
;;    5     1     5   1   B         1 X    
;;    6     4     6   1.5 C         2 X    
;;    7     1     7   0.5 A         1 X    
;;    8     4     8   1   B         2 X    
;;    9     1     9   1.5 C         1 X

;; ---- tech.ml.dataset

(def DS (ds/remove-column DS :v5))
DS
;; => _unnamed [9 6]:
;;    |   :V1 | :V2 |    :V3 | :V4 |   :v6 | :v7 |
;;    |-------+-----+--------+-----+-------+-----|
;;    | 1.000 |   1 | 0.5000 |   A | 1.000 |   X |
;;    | 4.000 |   2 |  1.000 |   B | 2.000 |   X |
;;    | 1.000 |   3 |  1.500 |   C | 1.000 |   X |
;;    | 4.000 |   4 | 0.5000 |   A | 2.000 |   X |
;;    | 1.000 |   5 |  1.000 |   B | 1.000 |   X |
;;    | 4.000 |   6 |  1.500 |   C | 2.000 |   X |
;;    | 1.000 |   7 | 0.5000 |   A | 1.000 |   X |
;;    | 4.000 |   8 |  1.000 |   B | 2.000 |   X |
;;    | 1.000 |   9 |  1.500 |   C | 1.000 |   X |

;; ### Remove several columns

;; ---- data.table

;; DT[, c("v6", "v7") := NULL]

(r '(bra ~DT nil ((rsymbol ":=") ["v6" "v7"] nil)))
;; =>    V1 V2  V3 V4
;;    1:  1  1 0.5  A
;;    2:  4  2 1.0  B
;;    3:  1  3 1.5  C
;;    4:  4  4 0.5  A
;;    5:  1  5 1.0  B
;;    6:  4  6 1.5  C
;;    7:  1  7 0.5  A
;;    8:  4  8 1.0  B
;;    9:  1  9 1.5  C

;; ---- dplyr

;; DF <- select(DF, -v6, -v7)

(def DF (dpl/select DF '(- v6) '(- v7)))
DF
;; => # A tibble: 9 x 4
;;         V1    V2    V3 V4   
;;      <dbl> <int> <dbl> <chr>
;;    1     1     1   0.5 A    
;;    2     4     2   1   B    
;;    3     1     3   1.5 C    
;;    4     4     4   0.5 A    
;;    5     1     5   1   B    
;;    6     4     6   1.5 C    
;;    7     1     7   0.5 A    
;;    8     4     8   1   B    
;;    9     1     9   1.5 C

;; ---- tech.ml.dataset

(def DS (ds/drop-columns DS [:v6 :v7]))
DS
;; => _unnamed [9 4]:
;;    |   :V1 | :V2 |    :V3 | :V4 |
;;    |-------+-----+--------+-----|
;;    | 1.000 |   1 | 0.5000 |   A |
;;    | 4.000 |   2 |  1.000 |   B |
;;    | 1.000 |   3 |  1.500 |   C |
;;    | 4.000 |   4 | 0.5000 |   A |
;;    | 1.000 |   5 |  1.000 |   B |
;;    | 4.000 |   6 |  1.500 |   C |
;;    | 1.000 |   7 | 0.5000 |   A |
;;    | 4.000 |   8 |  1.000 |   B |
;;    | 1.000 |   9 |  1.500 |   C |

;; ### Remove columns using a vector of colnames

;; ---- data.table

;; cols <- c("V3")
;; DT[, (cols) := NULL] # ! not DT[, cols := NULL]

(def cols (r.base/<- 'cols ["V3"]))
;; TODO (clojisr): enable wrapping into parantheses
;; hacking below
(r (str (:object-name DT) "[, (cols) := NULL]"))
;; =>    V1 V2 V4
;;    1:  1  1  A
;;    2:  4  2  B
;;    3:  1  3  C
;;    4:  4  4  A
;;    5:  1  5  B
;;    6:  4  6  C
;;    7:  1  7  A
;;    8:  4  8  B
;;    9:  1  9  C

;; ---- dplyr

;; cols <- c("V3")
;; DF <- select(DF, -one_of(cols))

(def cols ["V3"])
(def DF (dpl/select DF '(- (one_of ~cols))))
DF
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <dbl> <int> <chr>
;;    1     1     1 A    
;;    2     4     2 B    
;;    3     1     3 C    
;;    4     4     4 A    
;;    5     1     5 B    
;;    6     4     6 C    
;;    7     1     7 A    
;;    8     4     8 B    
;;    9     1     9 C    

;; ---- tech.ml.dataset

(def cols [:V3])
(def DS (ds/drop-columns DS cols))
DS
;; => _unnamed [9 3]:
;;    |   :V1 | :V2 | :V4 |
;;    |-------+-----+-----|
;;    | 1.000 |   1 |   A |
;;    | 4.000 |   2 |   B |
;;    | 1.000 |   3 |   C |
;;    | 4.000 |   4 |   A |
;;    | 1.000 |   5 |   B |
;;    | 4.000 |   6 |   C |
;;    | 1.000 |   7 |   A |
;;    | 4.000 |   8 |   B |
;;    | 1.000 |   9 |   C |

;; ### Replace values for rows matching a condition

;; ---- data.table

;; DT[V2 < 4, V2 := 0L]

(r/bra DT '(< V2 4) '((rsymbol ":=") V2 0))
;; =>    V1 V2 V4
;;    1:  1  0  A
;;    2:  4  0  B
;;    3:  1  0  C
;;    4:  4  4  A
;;    5:  1  5  B
;;    6:  4  6  C
;;    7:  1  7  A
;;    8:  4  8  B
;;    9:  1  9  C

;; ---- dplyr

;; DF <- mutate(DF, V2 = base::replace(V2, V2 < 4, 0L))

(def DF (dpl/mutate DF '(= V2 ((rsymbol base replace) V2 (< V2 4) 0))))
DF
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <dbl> <dbl> <chr>
;;    1     1     0 A    
;;    2     4     0 B    
;;    3     1     0 C    
;;    4     4     4 A    
;;    5     1     5 B    
;;    6     4     6 C    
;;    7     1     7 A    
;;    8     4     8 B    
;;    9     1     9 C

;; tech.ml.dataset

(def DS (ds/update-column DS :V2 #(map (fn [^long v]
                                         (if (< v 4) 0 v)) %)))
DS
;; => _unnamed [9 3]:
;;    |   :V1 |   :V2 | :V4 |
;;    |-------+-------+-----|
;;    | 1.000 | 0.000 |   A |
;;    | 4.000 | 0.000 |   B |
;;    | 1.000 | 0.000 |   C |
;;    | 4.000 | 4.000 |   A |
;;    | 1.000 | 5.000 |   B |
;;    | 4.000 | 6.000 |   C |
;;    | 1.000 | 7.000 |   A |
;;    | 4.000 | 8.000 |   B |
;;    | 1.000 | 9.000 |   C |

;; ## by

;; ### By group

;; ---- data.table

;; # one-liner:
;; DT[, .(sumV2 = sum(V2)), by = "V4"]
;; # reordered and indented:
;; DT[, by = V4,
;;    .(sumV2 = sum(V2))]

(r '(bra ~DT nil (. :sumV2 (sum V2)) :by "V4"))
;; =>    V4 sumV2
;;    1:  A    11
;;    2:  B    13
;;    3:  C    15

(r '(bra ~DT nil :by "V4" (. :sumV2 (sum V2))))
;; =>    V4 sumV2
;;    1:  A    11
;;    2:  B    13
;;    3:  C    15

;; ---- dplyr

;; DF %>%
;;    group_by(V4) %>%
;;    summarise(sumV2 = sum(V2))

(-> DF
    (dpl/group_by 'V4)
    (dpl/summarise :sumV2 '(sum V2)))
;; => # A tibble: 3 x 2
;;      V4    sumV2
;;      <chr> <dbl>
;;    1 A        11
;;    2 B        13
;;    3 C        15

;; ---- tech.ml.dataset

;; TODO: group-by and aggragete -> dataset
(group-by-columns-or-fn-and-aggregate [:V4] {:sumV2 #(dfn/sum (% :V2))} DS)
;; => _unnamed [3 2]:
;;    | :V4 | :sumV2 |
;;    |-----+--------|
;;    |   B |  13.00 |
;;    |   C |  15.00 |
;;    |   A |  11.00 |

;; ### By several groups

;; ---- data.table

;; DT[, keyby = .(V4, V1),
;;    .(sumV2 = sum(V2))]

(r '(bra ~DT nil :keyby (. V4 V1) (. :sumV2 (sum V2))))
;; =>    V4 V1 sumV2
;;    1:  A  1     7
;;    2:  A  4     4
;;    3:  B  1     5
;;    4:  B  4     8
;;    5:  C  1     9
;;    6:  C  4     6

;; ---- dplyr

;; DF %>%
;;   group_by(V4, V1) %>%
;;   summarise(sumV2 = sum(V2))

(-> DF
    (dpl/group_by 'V4 'V1)
    (dpl/summarise :sumV2 '(sum V2)))
;; => # A tibble: 6 x 3
;;    # Groups:   V4 [3]
;;      V4       V1 sumV2
;;      <chr> <dbl> <dbl>
;;    1 A         1     7
;;    2 A         4     4
;;    3 B         1     5
;;    4 B         4     8
;;    5 C         1     9
;;    6 C         4     6

;; ---- tech.ml.dataset

(->> (group-by-columns-or-fn-and-aggregate [:V4 :V1] {:sumV2 #(dfn/sum (% :V2))} DS)
     (sort-by-columns-with-orders [:V4 :V1]))
;; => _unnamed [6 3]:
;;    | :V4 |   :V1 | :sumV2 |
;;    |-----+-------+--------|
;;    |   A | 1.000 |  7.000 |
;;    |   A | 4.000 |  4.000 |
;;    |   B | 1.000 |  5.000 |
;;    |   B | 4.000 |  8.000 |
;;    |   C | 1.000 |  9.000 |
;;    |   C | 4.000 |  6.000 |

;; ### Calling function in by

;; ---- data.table

;; DT[, by = tolower(V4),
;;   .(sumV1 = sum(V1))]

(r '(bra ~DT nil :by (tolower V4) (. :sumV1 (sum V1))))
;; =>    tolower sumV1
;;    1:       a     6
;;    2:       b     9
;;    3:       c     6

;; ---- dplyr

;; DF %>%
;;   group_by(tolower(V4)) %>%
;;   summarise(sumV1 = sum(V1))

(-> DF
    (dpl/group_by '(tolower V4))
    (dpl/summarise :sumV1 '(sum V1)))
;; => # A tibble: 3 x 2
;;      `tolower(V4)` sumV1
;;      <chr>         <dbl>
;;    1 a                 6
;;    2 b                 9
;;    3 c                 6

;; ---- tech.ml.dataset

(->> (ds/update-column DS :V4 #(map str/lower-case %))
     (group-by-columns-or-fn-and-aggregate [:V4] {:sumV1 #(dfn/sum (% :V1))})
     (ds/sort-by-column :V4))
;; => _unnamed [3 2]:
;;    | :V4 | :sumV1 |
;;    |-----+--------|
;;    |   a |  6.000 |
;;    |   b |  9.000 |
;;    |   c |  6.000 |

;; ### Assigning column name in by

;; ---- data.table

;; DT[, keyby = .(abc = tolower(V4)),
;; .(sumV1 = sum(V1))]

(r '(bra ~DT nil :keyby (. :abc (tolower V4)) (. :sumV1 (sum V1))))
;; =>    abc sumV1
;;    1:   a     6
;;    2:   b     9
;;    3:   c     6

;; ---- dplyr

;; DF %>%
;;   group_by(abc = tolower(V4)) %>%
;;   summarise(sumV1 = sum(V1))

(-> DF
    (dpl/group_by :abc '(tolower V4))
    (dpl/summarise :sumV1 '(sum V1)))
;; => # A tibble: 3 x 2
;;      abc   sumV1
;;      <chr> <dbl>
;;    1 a         6
;;    2 b         9
;;    3 c         6

;; ---- tech.ml.dataset

(-> (ds/update-column DS :V4 #(map str/lower-case %))
    (ds/rename-columns {:V4 :abc})
    (->> (group-by-columns-or-fn-and-aggregate [:abc] {:sumV1 #(dfn/sum (% :V1))})
         (ds/sort-by-column :abc)))
;; => _unnamed [3 2]:
;;    | :abc | :sumV1 |
;;    |------+--------|
;;    |    a |  6.000 |
;;    |    b |  9.000 |
;;    |    c |  6.000 |

;; ### Using a condition in by

;; ---- data.table

;; DT[, keyby = V4 == "A", sum(V1)]

(r '(bra ~DT nil :keyby (== V4 "A") (sum V1)))
;; =>       V4 V1
;;    1: FALSE 15
;;    2:  TRUE  6

;; ---- dplyr

;; DF %>%
;;   group_by(V4 == "A") %>%
;;   summarise(sum(V1))

(-> DF
    (dpl/group_by '(== V4 "A"))
    (dpl/summarise '(sum V1)))
;; => # A tibble: 2 x 2
;;      `(V4 == "A")` `sum(V1)`
;;      <lgl>             <dbl>
;;    1 FALSE                15
;;    2 TRUE                  6

;; ---- tech.ml.dataset

(group-by-columns-or-fn-and-aggregate #(= (% :V4) \A)
                                      {:sumV1 #(dfn/sum (% :V1))}
                                      DS)
;; => _unnamed [2 2]:
;;    |  :_fn | :sumV1 |
;;    |-------+--------|
;;    | false |  15.00 |
;;    |  true |  6.000 |

;; ### By on a subset of rows

;; ---- data.table

;; DT[1:5,                # i
;;    .(sumV1 = sum(V1)), # j
;;    by = V4]            # by
;; ## complete DT[i, j, by] expression!

(r/bra DT (r/colon 1 5) '(. :sumV1 (sum V1)) :by 'V4)
;; =>    V4 sumV1
;;    1:  A     5
;;    2:  B     5
;;    3:  C     1

;; ---- dplyr

;; DF %>%
;;   slice(1:5) %>%
;;   group_by(V4) %>%
;;   summarise(sumV1 = sum(V1))

(-> DF
    (dpl/slice (r/colon 1 5))
    (dpl/group_by 'V4)
    (dpl/summarise :sumV1 '(sum V1)))
;; => # A tibble: 3 x 2
;;      V4    sumV1
;;      <chr> <dbl>
;;    1 A         5
;;    2 B         5
;;    3 C         1

;; ---- tech.ml.dataset

(->> (ds/select-rows DS (range 5))
     (group-by-columns-or-fn-and-aggregate [:V4] {:sumV1 #(dfn/sum (% :V1))})
     (ds/sort-by-column :V4))
;; => _unnamed [3 2]:
;;    | :V4 | :sumV1 |
;;    |-----+--------|
;;    |   A |  5.000 |
;;    |   B |  5.000 |
;;    |   C |  1.000 |

;; ### Count number of observations for each group

;; ---- data.table

;; DT[, .N, by = V4]

(r '(bra ~DT nil .N :by V4))
;; =>    V4 N
;;    1:  A 3
;;    2:  B 3
;;    3:  C 3

;; ---- dplyr

;; count(DF, V4)
;; DF %>%
;;   group_by(V4) %>%
;;   tally()
;; DF %>%
;;   group_by(V4) %>%
;;   summarise(n())
;; DF %>%
;;   group_by(V4) %>%
;;   group_size() # returns a vector

(dpl/count DF 'V4)
;; => # A tibble: 3 x 2
;;      V4        n
;;      <chr> <int>
;;    1 A         3
;;    2 B         3
;;    3 C         3

(-> DF (dpl/group_by 'V4) (dpl/tally))
;; => # A tibble: 3 x 2
;;      V4        n
;;      <chr> <int>
;;    1 A         3
;;    2 B         3
;;    3 C         3

(-> DF (dpl/group_by 'V4) (dpl/summarise '(n)))
;; => # A tibble: 3 x 2
;;      V4    `n()`
;;      <chr> <int>
;;    1 A         3
;;    2 B         3
;;    3 C         3

(-> DF (dpl/group_by 'V4) (dpl/group_size))
;; => [1] 3 3 3

;; ---- tech.ml.dataset

(group-by-columns-or-fn-and-aggregate [:V4] {:N ds/row-count} DS)
;; => _unnamed [3 2]:
;;    | :V4 | :N |
;;    |-----+----|
;;    |   B |  3 |
;;    |   C |  3 |
;;    |   A |  3 |

(map-v ds/row-count (ds/group-by-column :V4 DS))
;; => {\A 3, \B 3, \C 3}

(->> (vals (ds/group-by-column :V4 DS))
     (map ds/row-count))
;; => (3 3 3)

;; ### Add a column with number of observations for each group

;; ---- data.table

;; DT[, n := .N, by = V1][]
;; DT[, n := NULL] # rm column for consistency

(r '(bra ~DT nil ((rsymbol ":=") n .N) :by V1))
;; =>    V1 V2 V4 n
;;    1:  1  0  A 5
;;    2:  4  0  B 4
;;    3:  1  0  C 5
;;    4:  4  4  A 4
;;    5:  1  5  B 5
;;    6:  4  6  C 4
;;    7:  1  7  A 5
;;    8:  4  8  B 4
;;    9:  1  9  C 5

;; cleaning
(r '(bra ~DT nil ((rsymbol ":=") n nil)))
;; =>    V1 V2 V4
;;    1:  1  0  A
;;    2:  4  0  B
;;    3:  1  0  C
;;    4:  4  4  A
;;    5:  1  5  B
;;    6:  4  6  C
;;    7:  1  7  A
;;    8:  4  8  B
;;    9:  1  9  C

;; ---- dplyr

;; add_count(DF, V1)
;; DF %>%
;;   group_by(V1) %>%
;;   add_tally()

(dpl/add_count DF 'V1)
;; => # A tibble: 9 x 4
;;         V1    V2 V4        n
;;      <dbl> <dbl> <chr> <int>
;;    1     1     0 A         5
;;    2     4     0 B         4
;;    3     1     0 C         5
;;    4     4     4 A         4
;;    5     1     5 B         5
;;    6     4     6 C         4
;;    7     1     7 A         5
;;    8     4     8 B         4
;;    9     1     9 C         5

(-> DF
    (dpl/group_by 'V1)
    (dpl/add_tally))
;; => # A tibble: 9 x 4
;;    # Groups:   V1 [2]
;;         V1    V2 V4        n
;;      <dbl> <dbl> <chr> <int>
;;    1     1     0 A         5
;;    2     4     0 B         4
;;    3     1     0 C         5
;;    4     4     4 A         4
;;    5     1     5 B         5
;;    6     4     6 C         4
;;    7     1     7 A         5
;;    8     4     8 B         4
;;    9     1     9 C         5

;; ---- tech.ml.dataset

;; TODO: how to do this optimally and keep order?
(->> (ds/group-by identity [:V1] DS)
     (vals)
     (map (fn [ds]
            (let [rcnt (ds/row-count ds)]
              (ds/new-column ds :n (repeat rcnt rcnt)))))
     (apply ds/concat)
     (sort-by-columns-with-orders [:V2 :V4]))
;; => _unnamed [9 4]:
;;    |   :V1 |   :V2 | :V4 |    :n |
;;    |-------+-------+-----+-------|
;;    | 1.000 | 0.000 |   A | 5.000 |
;;    | 4.000 | 0.000 |   B | 4.000 |
;;    | 1.000 | 0.000 |   C | 5.000 |
;;    | 4.000 | 4.000 |   A | 4.000 |
;;    | 1.000 | 5.000 |   B | 5.000 |
;;    | 4.000 | 6.000 |   C | 4.000 |
;;    | 1.000 | 7.000 |   A | 5.000 |
;;    | 4.000 | 8.000 |   B | 4.000 |
;;    | 1.000 | 9.000 |   C | 5.000 |

;; ### Retrieve the first/last/nth observation for each group

;; ---- data.table

;; DT[, data.table::first(V2), by = V4]
;; DT[, data.table::last(V2), by = V4]
;; DT[, V2[2], by = V4]

(r '(bra ~DT nil ((rsymbol data.table first) V2) :by V4))
;; =>    V4 V1
;;    1:  A  0
;;    2:  B  0
;;    3:  C  0

(r '(bra ~DT nil ((rsymbol data.table last) V2) :by V4))
;; =>    V4 V1
;;    1:  A  7
;;    2:  B  8
;;    3:  C  9

(r '(bra ~DT nil (bra V2 2) :by V4))
;; =>    V4 V1
;;    1:  A  4
;;    2:  B  5
;;    3:  C  6

;; ---- dplyr

;; DF %>%
;;   group_by(V4) %>%
;;   summarise(dplyr::first(V2))
;; DF %>%
;;   group_by(V4) %>%
;;   summarise(dplyr::last(V2))
;; DF %>%
;;   group_by(V4) %>%
;;   summarise(dplyr::nth(V2, 2))

(-> DF (dpl/group_by 'V4) (dpl/summarise '((rsymbol dplyr first) V2)))
;; => # A tibble: 3 x 2
;;      V4    `dplyr::first(V2)`
;;      <chr>              <dbl>
;;    1 A                      0
;;    2 B                      0
;;    3 C                      0

(-> DF (dpl/group_by 'V4) (dpl/summarise '((rsymbol dplyr last) V2)))
;; => # A tibble: 3 x 2
;;      V4    `dplyr::last(V2)`
;;      <chr>             <dbl>
;;    1 A                     7
;;    2 B                     8
;;    3 C                     9

(-> DF (dpl/group_by 'V4) (dpl/summarise '((rsymbol dplyr nth) V2 2)))
;; => # A tibble: 3 x 2
;;      V4    `dplyr::nth(V2, 2)`
;;      <chr>               <dbl>
;;    1 A                       4
;;    2 B                       5
;;    3 C                       6

;; ---- tech.ml.dataset

(->> (group-by-columns-or-fn-and-aggregate [:V4] {:v #(first (% :V2))} DS)
     (ds/sort-by-column :V4))
;; => _unnamed [3 2]:
;;    | :V4 |    :v |
;;    |-----+-------|
;;    |   A | 0.000 |
;;    |   B | 0.000 |
;;    |   C | 0.000 |

(->> (group-by-columns-or-fn-and-aggregate [:V4] {:v #(last (% :V2))} DS)
     (ds/sort-by-column :V4))
;; => _unnamed [3 2]:
;;    | :V4 |    :v |
;;    |-----+-------|
;;    |   A | 7.000 |
;;    |   B | 8.000 |
;;    |   C | 9.000 |

(->> (group-by-columns-or-fn-and-aggregate [:V4] {:v #(nth (% :V2) 1)} DS)
     (ds/sort-by-column :V4))
;; => _unnamed [3 2]:
;;    | :V4 |    :v |
;;    |-----+-------|
;;    |   A | 4.000 |
;;    |   B | 5.000 |
;;    |   C | 6.000 |

;; -------------------------------

;; # Going further

;; ## Advanced columns manipulation

;; ### Summarise all the columns

;; ---- data.table

;; DT[, lapply(.SD, max)]

(r '(bra ~DT nil (lapply .SD max)))
;; =>    V1 V2 V4
;;    1:  4  9  C

;; ---- dplyr

;; summarise_all(DF, max)

(dpl/summarise_all DF r.base/max)
;; => # A tibble: 1 x 3
;;         V1    V2 V4   
;;      <dbl> <dbl> <chr>
;;    1     4     9 C

;; ---- tech.ml.dataset

;; TODO: dfn/max doesn't work

(apply-to-columns my-max :all DS)
;; => _unnamed [1 3]:
;;    |   :V1 |   :V2 | :V4 |
;;    |-------+-------+-----|
;;    | 4.000 | 9.000 |   C |

;; ### Summarise several columns

;; ---- data.table

;; DT[, lapply(.SD, mean),
;;    .SDcols = c("V1", "V2")]
;; # .SDcols is like "_at"

(r '(bra ~DT nil (lapply .SD mean) :.SDcols ["V1" "V2"]))
;; =>          V1       V2
;;    1: 2.333333 4.333333

;; ---- dplyr

;; summarise_at(DF, c("V1", "V2"), mean)

(dpl/summarise_at DF ["V1" "V2"] r.base/mean)
;; => # A tibble: 1 x 2
;;         V1    V2
;;      <dbl> <dbl>
;;    1  2.33  4.33

;; ---- tech.ml.dataset

;; doesn't work on beta 39
#_(->> (ds/select-columns DS [:V1 :V2])
       (apply-to-columns dfn/mean [:V1 :V2]))
;; => _unnamed [1 2]:
;;    |   :V1 |   :V2 |
;;    |-------+-------|
;;    | 2.333 | 4.333 |

;; ### Summarise several columns by group

;; ---- data.table

;; DT[, by = V4,
;;    lapply(.SD, mean),
;;    .SDcols = c("V1", "V2")]
;; ## using patterns (regex)
;; DT[, by = V4,
;;    lapply(.SD, mean),
;;    .SDcols = patterns("V1|V2")]

(r '(bra ~DT nil :by V4 (lapply .SD mean) :.SDcols ["V1" "V2"]))
;; =>    V4 V1       V2
;;    1:  A  2 3.666667
;;    2:  B  3 4.333333
;;    3:  C  2 5.000000

(r '(bra ~DT nil :by V4 (lapply .SD mean) :.SDcols (patterns "V1|V2")))
;; =>    V4 V1       V2
;;    1:  A  2 3.666667
;;    2:  B  3 4.333333
;;    3:  C  2 5.000000

;; ---- dplyr

;; DF %>%
;;   group_by(V4) %>%
;;   summarise_at(c("V1", "V2"), mean)
;; ## using select helpers
;; DF %>%
;;   group_by(V4) %>%
;;   summarise_at(vars(one_of("V1", "V2")), mean)

(-> DF (dpl/group_by 'V4) (dpl/summarise_at ["V1" "V2"] r.base/mean))
;; => # A tibble: 3 x 3
;;      V4       V1    V2
;;      <chr> <dbl> <dbl>
;;    1 A         2  3.67
;;    2 B         3  4.33
;;    3 C         2  5

(-> DF (dpl/group_by 'V4) (dpl/summarise_at '(vars (one_of "V1" "V2")) r.base/mean))
;; => # A tibble: 3 x 3
;;      V4       V1    V2
;;      <chr> <dbl> <dbl>
;;    1 A         2  3.67
;;    2 B         3  4.33
;;    3 C         2  5

;; ---- tech.ml.dataset

(->> DS
     (group-by-columns-or-fn-and-aggregate [:V4] {:V1 #(dfn/mean (% :V1))
                                                  :V2 #(dfn/mean (% :V2))})
     (ds/sort-by-column :V4))
;; => _unnamed [3 3]:
;;    | :V4 |   :V2 |   :V1 |
;;    |-----+-------+-------|
;;    |   A | 3.667 | 2.000 |
;;    |   B | 4.333 | 3.000 |
;;    |   C | 5.000 | 2.000 |

;; ### Summarise with more than one function by group

;; ---- data.table

;; DT[, by = V4, 
;;    c(lapply(.SD, sum),
;;    lapply(.SD, mean))]

(r '(bra ~DT nil :by V4 [(lapply .SD sum)
                         (lapply .SD mean)]))
;; =>    V4 V1 V2 V1       V2
;;    1:  A  6 11  2 3.666667
;;    2:  B  9 13  3 4.333333
;;    3:  C  6 15  2 5.000000

;; ---- dplyr

;; DF %>%
;;   group_by(V4) %>%
;;   summarise_all(list(sum, mean))

(-> DF (dpl/group_by 'V4) (dpl/summarise_all [:!list 'sum 'mean]))
;; => # A tibble: 3 x 5
;;      V4    V1_fn1 V2_fn1 V1_fn2 V2_fn2
;;      <chr>  <dbl>  <dbl>  <dbl>  <dbl>
;;    1 A          6     11      2   3.67
;;    2 B          9     13      3   4.33
;;    3 C          6     15      2   5

;; tech.ml.dataset

(->> DS
     (group-by-columns-or-fn-and-aggregate [:V4] {:V1-mean #(dfn/mean (% :V1))
                                                  :V2-mean #(dfn/mean (% :V2))
                                                  :V1-sum #(dfn/sum (% :V1))
                                                  :V2-sum #(dfn/sum (% :V2))})
     (ds/sort-by-column :V4))
;; => _unnamed [3 5]:
;;    | :V4 | :V2-sum | :V1-mean | :V1-sum | :V2-mean |
;;    |-----+---------+----------+---------+----------|
;;    |   A |   11.00 |    2.000 |   6.000 |    3.667 |
;;    |   B |   13.00 |    3.000 |   9.000 |    4.333 |
;;    |   C |   15.00 |    2.000 |   6.000 |    5.000 |

;; ### Summarise using a condition

;; ---- data.table

;; cols <- names(DT)[sapply(DT, is.numeric)]
;; DT[, lapply(.SD, mean),
;;    .SDcols = cols]

(def cols (r/bra (r.base/names DT) (r.base/sapply DT r.base/is-numeric)))
(r '(bra ~DT nil (lapply .SD mean) :.SDcols ~cols))
;; =>          V1       V2
;;    1: 2.333333 4.333333

;; ---- dplyr

;; summarise_if(DF, is.numeric, mean)

(dpl/summarise_if DF r.base/is-numeric r.base/mean)
;; => # A tibble: 1 x 2
;;         V1    V2
;;      <dbl> <dbl>
;;    1  2.33  4.33

;; ---- tech.ml.dataset

(def cols (->> DS
               (ds/columns)
               (map meta)
               (filter (comp #{:float64} :datatype))
               (map :name)))

(->> (ds/select-columns DS cols)
     (apply-to-columns dfn/mean cols))
;; => _unnamed [1 2]:
;;    |   :V1 |   :V2 |
;;    |-------+-------|
;;    | 2.333 | 4.333 |

;; ### Modify all the columns

;; ---- data.table

;; DT[, lapply(.SD, rev)]

(r '(bra ~DT nil (lapply .SD rev)))
;; =>    V1 V2 V4
;;    1:  1  9  C
;;    2:  4  8  B
;;    3:  1  7  A
;;    4:  4  6  C
;;    5:  1  5  B
;;    6:  4  4  A
;;    7:  1  0  C
;;    8:  4  0  B
;;    9:  1  0  A

;; ---- dplyr

;; mutate_all(DF, rev)
;; # transmute_all(DF, rev)

(dpl/mutate_all DF r.base/rev)
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <dbl> <dbl> <chr>
;;    1     1     9 C    
;;    2     4     8 B    
;;    3     1     7 A    
;;    4     4     6 C    
;;    5     1     5 B    
;;    6     4     4 A    
;;    7     1     0 C    
;;    8     4     0 B    
;;    9     1     0 A

;; ---- tech.ml.dataset

(ds/update-columns DS (ds/column-names DS) reverse)
;; => _unnamed [9 3]:
;;    |   :V1 |   :V2 | :V4 |
;;    |-------+-------+-----|
;;    | 1.000 | 9.000 |   C |
;;    | 4.000 | 8.000 |   B |
;;    | 1.000 | 7.000 |   A |
;;    | 4.000 | 6.000 |   C |
;;    | 1.000 | 5.000 |   B |
;;    | 4.000 | 4.000 |   A |
;;    | 1.000 | 0.000 |   C |
;;    | 4.000 | 0.000 |   B |
;;    | 1.000 | 0.000 |   A |

;; ### Modify several columns (dropping the others)

;; ---- data.table

;; DT[, lapply(.SD, sqrt),
;;    .SDcols = V1:V2]
;; DT[, lapply(.SD, exp),
;;    .SDcols = !"V4"]

(r '(bra ~DT nil (lapply .SD sqrt) :.SDcols (colon V1 V2)))
;; =>    V1       V2
;;    1:  1 0.000000
;;    2:  2 0.000000
;;    3:  1 0.000000
;;    4:  2 2.000000
;;    5:  1 2.236068
;;    6:  2 2.449490
;;    7:  1 2.645751
;;    8:  2 2.828427
;;    9:  1 3.000000

(r '(bra ~DT nil (lapply .SD exp) :.SDcols (! "V4")))
;; =>           V1         V2
;;    1:  2.718282    1.00000
;;    2: 54.598150    1.00000
;;    3:  2.718282    1.00000
;;    4: 54.598150   54.59815
;;    5:  2.718282  148.41316
;;    6: 54.598150  403.42879
;;    7:  2.718282 1096.63316
;;    8: 54.598150 2980.95799
;;    9:  2.718282 8103.08393

;; ---- dplyr

;; transmute_at(DF, c("V1", "V2"), sqrt)
;; transmute_at(DF, vars(-V4), exp)

(dpl/transmute_at DF ["V1" "V2"] 'sqrt)
;; => # A tibble: 9 x 2
;;         V1    V2
;;      <dbl> <dbl>
;;    1     1  0   
;;    2     2  0   
;;    3     1  0   
;;    4     2  2   
;;    5     1  2.24
;;    6     2  2.45
;;    7     1  2.65
;;    8     2  2.83
;;    9     1  3

(dpl/transmute_at DF '(vars (- V4)) 'exp)
;; => # A tibble: 9 x 2
;;         V1     V2
;;      <dbl>  <dbl>
;;    1  2.72    1  
;;    2 54.6     1  
;;    3  2.72    1  
;;    4 54.6    54.6
;;    5  2.72  148. 
;;    6 54.6   403. 
;;    7  2.72 1097. 
;;    8 54.6  2981. 
;;    9  2.72 8103.

;; ---- tech.ml. dataset

(->> (ds/select-columns DS [:V1 :V2])
     (apply-to-columns dfn/sqrt :all))
;; => _unnamed [9 2]:
;;    |   :V1 |   :V2 |
;;    |-------+-------|
;;    | 1.000 | 0.000 |
;;    | 2.000 | 0.000 |
;;    | 1.000 | 0.000 |
;;    | 2.000 | 2.000 |
;;    | 1.000 | 2.236 |
;;    | 2.000 | 2.449 |
;;    | 1.000 | 2.646 |
;;    | 2.000 | 2.828 |
;;    | 1.000 | 3.000 |

(->> (ds/drop-columns DS [:V4])
     (apply-to-columns dfn/exp :all))
;; => _unnamed [9 2]:
;;    |   :V1 |   :V2 |
;;    |-------+-------|
;;    | 2.718 | 1.000 |
;;    | 54.60 | 1.000 |
;;    | 2.718 | 1.000 |
;;    | 54.60 | 54.60 |
;;    | 2.718 | 148.4 |
;;    | 54.60 | 403.4 |
;;    | 2.718 |  1097 |
;;    | 54.60 |  2981 |
;;    | 2.718 |  8103 |

;; ### Modify several columns (keeping the others)

;; ---- data.table

;; DT[, c("V1", "V2") := lapply(.SD, sqrt),
;;    .SDcols = c("V1", "V2")]

;; cols <- setdiff(names(DT), "V4")
;; DT[, (cols) := lapply(.SD, "^", 2L),
;;    .SDcols = cols]

(r '(bra ~DT nil ((rsymbol ":=") ["V1" "V2"] (lapply .SD sqrt))
         :.SDcols ["V1" "V2"]))
;; =>    V1       V2 V4
;;    1:  1 0.000000  A
;;    2:  2 0.000000  B
;;    3:  1 0.000000  C
;;    4:  2 2.000000  A
;;    5:  1 2.236068  B
;;    6:  2 2.449490  C
;;    7:  1 2.645751  A
;;    8:  2 2.828427  B
;;    9:  1 3.000000  C

(def cols (r.base/<- 'cols (r.base/setdiff (r.base/names DT) "V4")))
(r (str (:object-name DT) "[, (cols) := lapply(.SD, \"^\", 2L), .SDcols = cols]"))
;; =>    V1 V2 V4
;;    1:  1  0  A
;;    2:  4  0  B
;;    3:  1  0  C
;;    4:  4  4  A
;;    5:  1  5  B
;;    6:  4  6  C
;;    7:  1  7  A
;;    8:  4  8  B
;;    9:  1  9  C

;; ---- dplyr

;; DF <- mutate_at(DF, c("V1", "V2"), sqrt)
;; DF <- mutate_at(DF, vars(-V4), "^", 2L)

(def DF (dpl/mutate_at DF ["V1" "V2"] 'sqrt))
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <dbl> <dbl> <chr>
;;    1     1  0    A    
;;    2     2  0    B    
;;    3     1  0    C    
;;    4     2  2    A    
;;    5     1  2.24 B    
;;    6     2  2.45 C    
;;    7     1  2.65 A    
;;    8     2  2.83 B    
;;    9     1  3    C

(def DF (dpl/mutate_at DF '(vars (- V4)) "^" 2))
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <dbl> <dbl> <chr>
;;    1     1  0    A    
;;    2     4  0    B    
;;    3     1  0    C    
;;    4     4  4    A    
;;    5     1  5.   B    
;;    6     4  6.00 C    
;;    7     1  7.   A    
;;    8     4  8.   B    
;;    9     1  9    C

;; ---- tech.ml.dataset

(def DS (apply-to-columns dfn/sqrt [:V1 :V2] DS))
;; => _unnamed [9 3]:
;;    |   :V1 |   :V2 | :V4 |
;;    |-------+-------+-----|
;;    | 1.000 | 0.000 |   A |
;;    | 2.000 | 0.000 |   B |
;;    | 1.000 | 0.000 |   C |
;;    | 2.000 | 2.000 |   A |
;;    | 1.000 | 2.236 |   B |
;;    | 2.000 | 2.449 |   C |
;;    | 1.000 | 2.646 |   A |
;;    | 2.000 | 2.828 |   B |
;;    | 1.000 | 3.000 |   C |

(def DS (apply-to-columns #(dfn/pow % 2.0) [:V1 :V2] DS))
;; => _unnamed [9 3]:
;;    |   :V1 |   :V2 | :V4 |
;;    |-------+-------+-----|
;;    | 1.000 | 0.000 |   A |
;;    | 4.000 | 0.000 |   B |
;;    | 1.000 | 0.000 |   C |
;;    | 4.000 | 4.000 |   A |
;;    | 1.000 | 5.000 |   B |
;;    | 4.000 | 6.000 |   C |
;;    | 1.000 | 7.000 |   A |
;;    | 4.000 | 8.000 |   B |
;;    | 1.000 | 9.000 |   C |

;; ### Modify columns using a condition (dropping the others)

;; ---- data.table

;; cols <- names(DT)[sapply(DT, is.numeric)]
;; DT[, .SD - 1,
;;    .SDcols = cols]

(def cols (r/bra (r.base/names DT) (r.base/sapply DT r.base/is-numeric)))

(r '(bra ~DT nil (- .SD 1) :.SDcols ~cols))
;; =>    V1 V2
;;    1:  0 -1
;;    2:  3 -1
;;    3:  0 -1
;;    4:  3  3
;;    5:  0  4
;;    6:  3  5
;;    7:  0  6
;;    8:  3  7
;;    9:  0  8


;; ---- dplyr

;; transmute_if(DF, is.numeric, list(~ '-'(., 1L)))

(dpl/transmute_if DF r.base/is-numeric [:!list '(formula nil (- . 1))])
;; => # A tibble: 9 x 2
;;         V1    V2
;;      <dbl> <dbl>
;;    1     0 -1   
;;    2     3 -1   
;;    3     0 -1   
;;    4     3  3   
;;    5     0  4.  
;;    6     3  5.00
;;    7     0  6.  
;;    8     3  7.  
;;    9     0  8   
;; ---- tech.ml.dataset

(def cols (->> DS
               (ds/columns)
               (map meta)
               (filter (comp #(= :float64 %) :datatype))
               (map :name)))

(->> (ds/select-columns DS cols)
     (apply-to-columns #(dfn/- % 1) cols))
;; => _unnamed [9 2]:
;;    |   :V1 |    :V2 |
;;    |-------+--------|
;;    | 0.000 | -1.000 |
;;    | 3.000 | -1.000 |
;;    | 0.000 | -1.000 |
;;    | 3.000 |  3.000 |
;;    | 0.000 |  4.000 |
;;    | 3.000 |  5.000 |
;;    | 0.000 |  6.000 |
;;    | 3.000 |  7.000 |
;;    | 0.000 |  8.000 |

;; ### Modify columns using a condition (keeping the others)

;; ---- data.table

;; DT[, (cols) := lapply(.SD, as.integer),
;; .SDcols = cols]

(def cols (r.base/<- 'cols (r.base/setdiff (r.base/names DT) "V4")))
(r (str (:object-name DT) "[, (cols) := lapply(.SD, as.integer), .SDcols = cols]"))
;; =>    V1 V2 V4
;;    1:  1  0  A
;;    2:  4  0  B
;;    3:  1  0  C
;;    4:  4  4  A
;;    5:  1  5  B
;;    6:  4  5  C
;;    7:  1  7  A
;;    8:  4  8  B
;;    9:  1  9  C

;; ---- dplyr

;; DF <- mutate_if(DF, is.numeric, as.integer)

(def DF (dpl/mutate_if DF r.base/is-numeric r.base/as-integer))
;; => # A tibble: 9 x 3
;;         V1    V2 V4   
;;      <int> <int> <chr>
;;    1     1     0 A    
;;    2     4     0 B    
;;    3     1     0 C    
;;    4     4     4 A    
;;    5     1     5 B    
;;    6     4     5 C    
;;    7     1     7 A    
;;    8     4     8 B    
;;    9     1     9 C

;; ----tech.ml.dataset

;; TODO: easier cast to given type
(def DS (apply-to-columns #(dtype/->reader % :int64) [:V1 :V2] DS))
;; => _unnamed [9 3]:
;;    | :V1 | :V2 | :V4 |
;;    |-----+-----+-----|
;;    |   1 |   0 |   A |
;;    |   4 |   0 |   B |
;;    |   1 |   0 |   C |
;;    |   4 |   4 |   A |
;;    |   1 |   5 |   B |
;;    |   4 |   5 |   C |
;;    |   1 |   7 |   A |
;;    |   4 |   8 |   B |
;;    |   1 |   9 |   C |

;; ### Use a complex expression

;; ---- data.table

;; DT[, by = V4, .(V1[1:2], "X")]

(r '(bra ~DT nil :by V4 (. (bra V1 (colon 1 2)) "X")))
;; =>    V4 V1 V2
;;    1:  A  1  X
;;    2:  A  4  X
;;    3:  B  4  X
;;    4:  B  1  X
;;    5:  C  1  X
;;    6:  C  4  X

;; ---- dplyr

;; DF %>%
;;   group_by(V4) %>%
;;   slice(1:2) %>%
;;   transmute(V1 = V1,
;;             V2 = "X")

(-> DF
    (dpl/group_by 'V4)
    (dpl/slice (r/colon 1 2))
    (dpl/transmute :V1 'V1 :V2 "X"))
;; => # A tibble: 6 x 3
;;    # Groups:   V4 [3]
;;      V4       V1 V2   
;;      <chr> <int> <chr>
;;    1 A         1 X    
;;    2 A         4 X    
;;    3 B         4 X    
;;    4 B         1 X    
;;    5 C         1 X    
;;    6 C         4 X    

;; ---- tech.ml.dataset

(->> (ds/group-by-column :V4 DS)
     (vals)
     (map #(ds/head 2 %))
     (map #(ds/add-or-update-column % :V2 (repeat (ds/row-count %) "X")))
     (apply ds/concat))
;; => null [6 3]:
;;    | :V1 | :V2 | :V4 |
;;    |-----+-----+-----|
;;    |   1 |   X |   A |
;;    |   4 |   X |   A |
;;    |   4 |   X |   B |
;;    |   1 |   X |   B |
;;    |   1 |   X |   C |
;;    |   4 |   X |   C |

;; ### Use multiple expressions (with DT[,{j}])

;; ---- data.table

;; DT[, {print(V1) #  comments here!
;;       print(summary(V1))
;;       x <- V1 + sum(V2)
;;       .(A = 1:.N, B = x) # last list returned as a data.table
;;       }]

(r (str (:object-name DT)
        "[, {print(V1) #  comments here!
             print(summary(V1))
             x <- V1 + sum(V2)
             .(A = 1:.N, B = x)}]"))
;; =>    A  B
;;    1: 1 39
;;    2: 2 42
;;    3: 3 39
;;    4: 4 42
;;    5: 5 39
;;    6: 6 42
;;    7: 7 39
;;    8: 8 42
;;    9: 9 39

;; printed:
;;   [1] 1 4 1 4 1 4 1 4 1
;;   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
;;   1.000   1.000   1.000   2.333   4.000   4.000 

;; ---- dplyr

;; no provided implementation

;; ---- tech.ml.dataset

(let [x (dfn/+ (DS :V1) (dfn/sum (DS :V2)))]
  (println (DS :V1))
  (println (dfn/descriptive-stats (DS :V2)))
  (ds/name-values-seq->dataset {:A (map inc (range (ds/row-count DS)))
                                :B x}))
;; => _unnamed [9 2]:
;;    | :A |    :B |
;;    |----+-------|
;;    |  1 | 39.00 |
;;    |  2 | 42.00 |
;;    |  3 | 39.00 |
;;    |  4 | 42.00 |
;;    |  5 | 39.00 |
;;    |  6 | 42.00 |
;;    |  7 | 39.00 |
;;    |  8 | 42.00 |
;;    |  9 | 39.00 |

;; printed:

;; #tech.ml.dataset.column<int64>[9]
;; :V1
;; [1, 4, 1, 4, 1, 4, 1, 4, 1, ]
;; {:min 0.0, :mean 4.222222222222222, :skew -0.1481546009077147, :ecount 9, :standard-deviation 3.527668414752787, :median 5.0, :max 9.0}


