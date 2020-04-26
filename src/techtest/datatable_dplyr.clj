(ns techtest.datatable-dplyr
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [fastmath.core :as m]
            [clojisr.v1.r :as r :refer [r r->clj]]
            [clojisr.v1.require :refer [require-r]]))

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
(require-r '[base]
           '[utils]
           '[stats]
           '[tidyr]
           '[dplyr :as dpl]
           '[data.table :as dt])
(r.base/options :width 160)
(r.base/set-seed 1)

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
(r/bra DT '(~(r/rsymbol "%chin%") V4 ["A" "C"]))
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

(ds/filter-column #(> % 5) :V2 DS)
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

;; TODO (tech.ml.dataset): keep the order
(ds/unique-by identity DS :column-name-seq (ds/column-names DS))
;; => _unnamed [9 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   2 |   8 |  1.000 |   B |
;;    |   1 |   5 |  1.000 |   B |
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   2 |  1.000 |   B |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   1 |   1 | 0.5000 |   A |
;;    |   1 |   7 | 0.5000 |   A |
;;    |   1 |   9 |  1.500 |   C |

(ds/unique-by identity DS :column-name-seq [:V1 :V4])
;; => _unnamed [6 4]:
;;    | :V1 | :V2 |    :V3 | :V4 |
;;    |-----+-----+--------+-----|
;;    |   2 |   6 |  1.500 |   C |
;;    |   1 |   1 | 0.5000 |   A |
;;    |   1 |   5 |  1.000 |   B |
;;    |   1 |   3 |  1.500 |   C |
;;    |   2 |   4 | 0.5000 |   A |
;;    |   2 |   2 |  1.000 |   B |

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

;; note: here we use `fastmath` to calculate rank. There is no `dense` method but `min` makes a work.
;;       We need to translate rank to indices.

(defn filter-by-rank-indices
  [pred rank]
  (->> rank
       (map-indexed vector)
       (filter (comp pred second))
       (map first)))

(->> (m/rank (map - (DS :V1)) :dense)
     (filter-by-rank-indices #(< % 1))
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

(r/bra DT '(~(r/rsymbol "%like%") V4 "^B"))
;; =>    V1 V2 V3 V4
;;    1:  2  2  1  B
;;    2:  1  5  1  B
;;    3:  2  8  1  B

(r/bra DT '(~(r/rsymbol "%between%") V2 [3 5]))
;; =>    V1 V2  V3 V4
;;    1:  1  3 1.5  C
;;    2:  2  4 0.5  A
;;    3:  1  5 1.0  B

(r/bra DT '(~(r/rsymbol 'data.table 'between) V2 3 5 :incbounds false))
;; =>    V1 V2  V3 V4
;;    1:  2  4 0.5  A

(r/bra DT '(~(r/rsymbol "%inrange%") V2 [:!list (colon -1 1) (colon 1 3)]))
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

(dpl/filter DF '(~(r/rsymbol 'dplyr 'between) V2 3 5))
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

;; tech.ml.dataset

;; TODO: improve sorting

(defn asc-desc-comparator
  [orders]
  (if (every? #(= % :asc) orders)
    compare
    (let [mults (map #(if (= % :asc) 1 -1) orders)]
      (fn [v1 v2]
        (reduce (fn [_ [a b mult]]
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
