(ns techtest.core
  (:require [tech.ml.dataset :as ds]
            [tech.ml.dataset.column :as col]
            [tech.v2.datatype.functional :as dfn]
            [clojisr.v1.r :as r :refer [r]]
            [clojisr.v1.require :refer [require-r]]))

;; created with tech.ml.dataset version "2.0-beta-24"
;; further versions may simplify some things

;; Working thrugh R `data.table` type and confronting with tech.ml.dataset
;; https://cran.r-project.org/web/packages/data.table/vignettes/datatable-intro.html

;; # Preparation

;; load R package
(require-r '[base]
           '[utils]
           '[data.table :as dt])
(r.base/options :width 160)

;; # Read data from URL

;; input <- "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv"
;; flights <- fread(input)

;; --------- R

(def R-flights (time (dt/fread "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv")))

;; --------- Clojure

(def flights (time (ds/->dataset "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv")))

;; # Taking the shape of loaded data

;; dim(flights)

;; --------- R

(r.base/dim R-flights)
;; => [1] 253316     11

;; --------- Clojure
(ds/column-count flights) ;; => 11
(ds/row-count flights) ;; => 253316

;; TODO: maybe add those numbers to a metadata? Like in `column` case?
(meta flights) 
;; => {:name "https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv"}

;; # Basics

;; ## What is `data.table`?

;; DT = data.table(
;;   ID = c("b","b","b","a","a","c"),
;;   a = 1:6,
;;   b = 7:12,
;;   c = 13:18
;; )
;; 
;; class(DT$ID)

;; --------- R

(def R-DT (dt/data-table :ID ["b" "b" "b" "c" "c" "a"]
                         :a (r/colon 1 6)
                         :b (r/colon 7 12)
                         :c (r/colon 13 18)))

R-DT
;; =>    ID a  b  c
;;    1:  b 1  7 13
;;    2:  b 2  8 14
;;    3:  b 3  9 15
;;    4:  c 4 10 16
;;    5:  c 5 11 17
;;    6:  a 6 12 18

(r.base/class (r.base/$ R-DT 'ID))
;; => [1] "character"

;; --------- Clojure

(def DT (ds/name-values-seq->dataset {:ID ["b" "b" "b" "c" "c" "a"]
                                      :a (range 1 7)
                                      :b (range 7 13)
                                      :c (range 13 19)}))

DT
;; => _unnamed [6 4]:
;;    | :ID | :a | :b | :c |
;;    |-----+----+----+----|
;;    |   b |  1 |  7 | 13 |
;;    |   b |  2 |  8 | 14 |
;;    |   b |  3 |  9 | 15 |
;;    |   c |  4 | 10 | 16 |
;;    |   c |  5 | 11 | 17 |
;;    |   a |  6 | 12 | 18 |

(meta (DT :ID))
;; => {:categorical? true, :name :ID, :size 6, :datatype :string}

(:datatype (meta (ds/column DT :ID)))
;; => :string

;; # Subset rows

;; ## Get all the flights with “JFK” as the origin airport in the month of June.

;; ans <- flights[origin == "JFK" & month == 6L]

;; --------- R

(def ans (r/bra R-flights '(& (== origin "JFK")
                              (== month 6))))


(r.utils/head ans)
;; =>    year month day dep_delay arr_delay carrier origin dest air_time distance hour
;;    1: 2014     6   1        -9        -5      AA    JFK  LAX      324     2475    8
;;    2: 2014     6   1       -10       -13      AA    JFK  LAX      329     2475   12
;;    3: 2014     6   1        18        -1      AA    JFK  LAX      326     2475    7
;;    4: 2014     6   1        -6       -16      AA    JFK  LAX      320     2475   10
;;    5: 2014     6   1        -4       -45      AA    JFK  LAX      326     2475   18
;;    6: 2014     6   1        -6       -23      AA    JFK  LAX      329     2475   14

;; --------- Clojure

(def ans (ds/filter #(and (= (get % "origin") "JFK")
                          (= (get % "month") 6)) flights))

;; TODO: maybe add head/tail for rows? or accept positive number (for head) and negative number (for tail) `
(ds/select-rows ans (range 6))
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [6 11]:
;;    | year | month | day | dep_delay | arr_delay | carrier | origin | dest | air_time | distance | hour |
;;    |------+-------+-----+-----------+-----------+---------+--------+------+----------+----------+------|
;;    | 2014 |     6 |   1 |        -9 |        -5 |      AA |    JFK |  LAX |      324 |     2475 |    8 |
;;    | 2014 |     6 |   1 |       -10 |       -13 |      AA |    JFK |  LAX |      329 |     2475 |   12 |
;;    | 2014 |     6 |   1 |        18 |        -1 |      AA |    JFK |  LAX |      326 |     2475 |    7 |
;;    | 2014 |     6 |   1 |        -6 |       -16 |      AA |    JFK |  LAX |      320 |     2475 |   10 |
;;    | 2014 |     6 |   1 |        -4 |       -45 |      AA |    JFK |  LAX |      326 |     2475 |   18 |
;;    | 2014 |     6 |   1 |        -6 |       -23 |      AA |    JFK |  LAX |      329 |     2475 |   14 |

;; ## Get first two rows from `flights`

;; ans <- flights[1:2]

;; --------- R

(def ans (r/bra R-flights (r/colon 1 2)))

ans
;; =>    year month day dep_delay arr_delay carrier origin dest air_time distance hour
;;    1: 2014     1   1        14        13      AA    JFK  LAX      359     2475    9
;;    2: 2014     1   1        -3        13      AA    JFK  LAX      363     2475   11

;; --------- Clojure

(def ans (ds/select-rows flights (range 2)))

ans
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [2 11]:
;;    | year | month | day | dep_delay | arr_delay | carrier | origin | dest | air_time | distance | hour |
;;    |------+-------+-----+-----------+-----------+---------+--------+------+----------+----------+------|
;;    | 2014 |     1 |   1 |        14 |        13 |      AA |    JFK |  LAX |      359 |     2475 |    9 |
;;    | 2014 |     1 |   1 |        -3 |        13 |      AA |    JFK |  LAX |      363 |     2475 |   11 |

;; ## Sort `flights` first by column `origin` in ascending order, and then by `dest` in descending order

;; ans <- flights[order(origin, -dest)]

;; --------- R

(def ans (r/bra R-flights '(order origin (- dest))))

(r.utils/head ans)
;; =>    year month day dep_delay arr_delay carrier origin dest air_time distance hour
;;    1: 2014     1   5         6        49      EV    EWR  XNA      195     1131    8
;;    2: 2014     1   6         7        13      EV    EWR  XNA      190     1131    8
;;    3: 2014     1   7        -6       -13      EV    EWR  XNA      179     1131    8
;;    4: 2014     1   8        -7       -12      EV    EWR  XNA      184     1131    8
;;    5: 2014     1   9        16         7      EV    EWR  XNA      181     1131    8
;;    6: 2014     1  13        66        66      EV    EWR  XNA      188     1131    9

;; --------- Clojure

;; TODO: below I want to sort dataset by origin ascending and dest descending. Maybe such case should be
;;       wrapped into the function? Writing comparators is not convinient in this quite common case.

(defn string-pair-comparator
  [[o1 d1] [o2 d2]]
  (let [compare-first (compare o1 o2)]
    (if-not (zero? compare-first)
      compare-first
      (- (compare d1 d2)))))

(def ans (ds/sort-by #(vector (get % "origin")
                              (get % "dest"))
                     string-pair-comparator flights))

(ds/select-rows ans (range 6))
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [6 11]:
;;    | year | month | day | dep_delay | arr_delay | carrier | origin | dest | air_time | distance | hour |
;;    |------+-------+-----+-----------+-----------+---------+--------+------+----------+----------+------|
;;    | 2014 |     6 |   3 |        -6 |       -38 |      EV |    EWR |  XNA |      154 |     1131 |    6 |
;;    | 2014 |     1 |  20 |        -9 |       -17 |      EV |    EWR |  XNA |      177 |     1131 |    8 |
;;    | 2014 |     3 |  19 |        -6 |        10 |      EV |    EWR |  XNA |      201 |     1131 |    6 |
;;    | 2014 |     2 |   3 |       231 |       268 |      EV |    EWR |  XNA |      184 |     1131 |   12 |
;;    | 2014 |     4 |  25 |        -8 |       -32 |      EV |    EWR |  XNA |      159 |     1131 |    6 |
;;    | 2014 |     2 |  19 |        21 |        10 |      EV |    EWR |  XNA |      176 |     1131 |    8 |

;; # Select column(s)

;; ## Select `arr_delay` column, but return it as a vector.

;; ans <- flights[, arr_delay]

;; --------- R

;; this should work but we have a bug in `clojisr` (addressed)
;; (def ans (r/bra R-flights nil 'arr_delay))
(def ans (r '(bra ~R-flights nil arr_delay)))

(r.utils/head ans)
;; => [1]  13  13   9 -26   1   0

;; --------- Clojure

(def ans (flights "arr_delay"))

(take 6 ans)
;; => (13 13 9 -26 1 0)

;; or

(def ans (ds/column flights "arr_delay"))

(take 6 ans)
;; => (13 13 9 -26 1 0)

;; ## Select `arr_delay` column, but return as a data.table instead

;; ans <- flights[, list(arr_delay)]

;; --------- R

(def ans (r '(bra ~R-flights nil [:!list arr_delay])))

(r.utils/head ans)
;; =>    arr_delay
;;    1:        13
;;    2:        13
;;    3:         9
;;    4:       -26
;;    5:         1
;;    6:         0

;; --------- Clojure

(def ans (ds/select-columns flights ["arr_delay"]))

(ds/select-rows ans (range 6))
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [6 1]:
;;    | arr_delay |
;;    |-----------|
;;    |        13 |
;;    |        13 |
;;    |         9 |
;;    |       -26 |
;;    |         1 |
;;    |         0 |

;; TODO: question: consider IFn returns dataset by default. Arguments:
;; * column name - returns dataset with single column
;; * sequence of columns - returns dataset with selected columns
;; * map - returns dataset with selected and renamed columns

;; ## Select both `arr_delay` and `dep_delay` columns

;; ans <- flights[, .(arr_delay, dep_delay)]
;; ans <- flights[, list(arr_delay, dep_delay)]

;; --------- R

(def ans (r '(bra ~R-flights nil (. arr_delay dep_delay))))

(r.utils/head ans)
;; =>    arr_delay dep_delay
;;    1:        13        14
;;    2:        13        -3
;;    3:         9         2
;;    4:       -26        -8
;;    5:         1         2
;;    6:         0         4

;; or

(def ans (r '(bra ~R-flights nil [:!list arr_delay dep_delay])))

(r.utils/head ans)
;; =>    arr_delay dep_delay
;;    1:        13        14
;;    2:        13        -3
;;    3:         9         2
;;    4:       -26        -8
;;    5:         1         2
;;    6:         0         4

;; --------- Clojure

(def ans (ds/select-columns flights ["arr_delay" "dep_delay"]))

(ds/select-rows ans (range 6))
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [6 2]:
;;    | arr_delay | dep_delay |
;;    |-----------+-----------|
;;    |        13 |        14 |
;;    |        13 |        -3 |
;;    |         9 |         2 |
;;    |       -26 |        -8 |
;;    |         1 |         2 |
;;    |         0 |         4 |

;; ## Select both `arr_delay` and `dep_delay` columns and rename them to `delay_arr` and `delay_dep`

;; ans <- flights[, .(delay_arr = arr_delay, delay_dep = dep_delay)]

;; --------- R

(def ans (r '(bra ~R-flights nil (. :delay_arr arr_delay :delay_dep dep_delay))))

(r.utils/head ans)
;; =>    delay_arr delay_dep
;;    1:        13        14
;;    2:        13        -3
;;    3:         9         2
;;    4:       -26        -8
;;    5:         1         2
;;    6:         0         4

;; --------- Clojure

;; TODO: Propose to do as in R, when you provide a map to `select` names will be changed
(def ans (-> (ds/select-columns flights ["arr_delay" "dep_delay"])
             (ds/rename-columns  {"arr_delay" "delay_arr"
                                  "dep_delay" "delay_dep"})))

(ds/select-rows ans (range 6))
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [6 2]:
;;    | delay_arr | delay_dep |
;;    |-----------+-----------|
;;    |        13 |        14 |
;;    |        13 |        -3 |
;;    |         9 |         2 |
;;    |       -26 |        -8 |
;;    |         1 |         2 |
;;    |         0 |         4 |

;; ## How many trips have had total delay < 0?

;; ans <- flights[, sum( (arr_delay + dep_delay) < 0 )]

;; --------- R

(def ans (r '(bra ~R-flights nil (sum (< (+ arr_delay dep_delay) 0)))))

ans
;; => [1] 141814

;; --------- Clojure

;; TODO: maybe ds should be also countable?

(def ans (-> (ds/filter #(neg? (+ (get % "arr_delay")
                                  (get % "dep_delay"))) flights)
             (ds/row-count)))

ans
;; => 141814

;; ## Calculate the average arrival and departure delay for all flights with “JFK” as the origin airport in the month of June.

;; ans <- flights[origin == "JFK" & month == 6L,
;;                .(m_arr = mean(arr_delay), m_dep = mean(dep_delay))]

;; --------- R

(def ans (r/bra R-flights
                '(& (== origin "JFK")
                    (== month 6))
                '(. :m_arr (mean arr_delay)
                    :m_dep (mean dep_delay))))

ans
;; =>       m_arr    m_dep
;;    1: 5.839349 9.807884

;; --------- Clojure

;; TODO: I would prefer to get dataset here

(def ans (->> (-> (ds/filter #(and (= (get % "origin") "JFK")
                                   (= (get % "month") 6)) flights)
                  (ds/select-columns ["arr_delay" "dep_delay"]))
              (map dfn/mean)))

ans
;; => (5.839349323200929 9.807884113037279)

;; or

(defn aggregate
([agg-fns-map ds]
 (aggregate {} agg-fns-map ds))
([m agg-fns-map ds]
 (into m (map (fn [[k agg-fn]]
                [k (agg-fn ds)]) agg-fns-map))))

(def aggregate->dataset (comp ds/->dataset vector aggregate))

(def ans (->> (-> (ds/filter #(and (= (get % "origin") "JFK")
                                   (= (get % "month") 6)) flights)
                  (ds/select-columns ["arr_delay" "dep_delay"]))
              (aggregate->dataset {:m_arr #(dfn/mean (ds/column % "arr_delay"))
                                   :m_dep #(dfn/mean (ds/column % "dep_delay"))})))

ans
;; => _unnamed [1 2]:
;;    | :m_arr | :m_dep |
;;    |--------+--------|
;;    |  5.839 |  9.808 |

;; ## How many trips have been made in 2014 from “JFK” airport in the month of June?

;; ans <- flights[origin == "JFK" & month == 6L, length(dest)]
;; ans <- flights[origin == "JFK" & month == 6L, .N]

;; --------- R

(def ans (r/bra R-flights
                '(& (== origin "JFK")
                    (== month 6))
                '(length dest)))

ans
;; => [1] 8422

;; or

(def ans (r/bra R-flights
                '(& (== origin "JFK")
                    (== month 6))
                '.N))

ans
;; => [1] 8422

;; --------- Clojure

(def ans (-> (ds/filter #(and (= (get % "origin") "JFK")
                              (= (get % "month") 6)) flights)
             (ds/row-count)))

ans
;; => 8422

;; ## Select both arr_delay and dep_delay columns the data.frame way.
;; ## Select columns named in a variable using the .. prefix
;; ## Select columns named in a variable using with = FALSE

;; ans <- flights[, c("arr_delay", "dep_delay")]
;; select_cols = c("arr_delay", "dep_delay")
;;   flights[ , ..select_cols]
;; flights[ , select_cols, with = FALSE]

;; --------- R

(def ans (r '(bra ~R-flights nil ["arr_delay" "dep_delay"])))

(r.utils/head ans)
;; =>    arr_delay dep_delay
;;    1:        13        14
;;    2:        13        -3
;;    3:         9         2
;;    4:       -26        -8
;;    5:         1         2
;;    6:         0         4

;; or

(def select_cols (r.base/<- 'select_cols ["arr_delay" "dep_delay"]))

(r '(bra ~R-flights nil ..select_cols))
;; =>         arr_delay dep_delay
;;         1:        13        14
;;         2:        13        -3
;;         3:         9         2
;;         4:       -26        -8
;;         5:         1         2
;;        ---                    
;;    253312:       -30         1
;;    253313:       -14        -5
;;    253314:        16        -8
;;    253315:        15        -4
;;    253316:         1        -5

;; or

(r '(bra ~R-flights nil select_cols :with false))
;; =>         arr_delay dep_delay
;;         1:        13        14
;;         2:        13        -3
;;         3:         9         2
;;         4:       -26        -8
;;         5:         1         2
;;        ---                    
;;    253312:       -30         1
;;    253313:       -14        -5
;;    253314:        16        -8
;;    253315:        15        -4
;;    253316:         1        -5

;; --------- Clojure

(def select_cols ["arr_delay" "dep_delay"])

(ds/select-columns flights select_cols)
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [253316 2]:
;;    | arr_delay | dep_delay |
;;    |-----------+-----------|
;;    |        13 |        14 |
;;    |        13 |        -3 |
;;    |         9 |         2 |
;;    |       -26 |        -8 |
;;    |         1 |         2 |
;;    |         0 |         4 |
;;    |       -18 |        -2 |
;;    |       -14 |        -3 |
;;    |       -17 |        -1 |
;;    |       -14 |        -2 |
;;    |       -17 |        -5 |
;;    |        -5 |         7 |
;;    |         1 |         3 |
;;    |       133 |       142 |
;;    |       -26 |        -5 |
;;    |        69 |        18 |
;;    |        36 |        25 |
;;    |         1 |        -1 |
;;    |       185 |       191 |
;;    |        -6 |        -7 |
;;    |         0 |        -7 |
;;    |       -17 |        -8 |
;;    |        15 |        -2 |
;;    |         1 |        -3 |
;;    |        42 |        44 |

;; ## Deselect columns

;; ans <- flights[, !c("arr_delay", "dep_delay")]
;; ans <- flights[, -c("arr_delay", "dep_delay")]

;; --------- R

(r '(bra ~R-flights nil (! ["arr_delay" "dep_delay"])))
;; =>         year month day carrier origin dest air_time distance hour
;;         1: 2014     1   1      AA    JFK  LAX      359     2475    9
;;         2: 2014     1   1      AA    JFK  LAX      363     2475   11
;;         3: 2014     1   1      AA    JFK  LAX      351     2475   19
;;         4: 2014     1   1      AA    LGA  PBI      157     1035    7
;;         5: 2014     1   1      AA    JFK  LAX      350     2475   13
;;        ---                                                          
;;    253312: 2014    10  31      UA    LGA  IAH      201     1416   14
;;    253313: 2014    10  31      UA    EWR  IAH      189     1400    8
;;    253314: 2014    10  31      MQ    LGA  RDU       83      431   11
;;    253315: 2014    10  31      MQ    LGA  DTW       75      502   11
;;    253316: 2014    10  31      MQ    LGA  SDF      110      659    8

;; or

(r '(bra ~R-flights nil (- ["arr_delay" "dep_delay"])))
;; =>         year month day carrier origin dest air_time distance hour
;;         1: 2014     1   1      AA    JFK  LAX      359     2475    9
;;         2: 2014     1   1      AA    JFK  LAX      363     2475   11
;;         3: 2014     1   1      AA    JFK  LAX      351     2475   19
;;         4: 2014     1   1      AA    LGA  PBI      157     1035    7
;;         5: 2014     1   1      AA    JFK  LAX      350     2475   13
;;        ---                                                          
;;    253312: 2014    10  31      UA    LGA  IAH      201     1416   14
;;    253313: 2014    10  31      UA    EWR  IAH      189     1400    8
;;    253314: 2014    10  31      MQ    LGA  RDU       83      431   11
;;    253315: 2014    10  31      MQ    LGA  DTW       75      502   11
;;    253316: 2014    10  31      MQ    LGA  SDF      110      659    8


;; --------- Clojure

(ds/remove-columns flights ["arr_delay" "dep_delay"])
;; => https://raw.githubusercontent.com/Rdatatable/data.table/master/vignettes/flights14.csv [253316 9]:
;;    | year | month | day | carrier | origin | dest | air_time | distance | hour |
;;    |------+-------+-----+---------+--------+------+----------+----------+------|
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      359 |     2475 |    9 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      363 |     2475 |   11 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      351 |     2475 |   19 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  PBI |      157 |     1035 |    7 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      350 |     2475 |   13 |
;;    | 2014 |     1 |   1 |      AA |    EWR |  LAX |      339 |     2454 |   18 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      338 |     2475 |   21 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      356 |     2475 |   15 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  MIA |      161 |     1089 |   15 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  SEA |      349 |     2422 |   18 |
;;    | 2014 |     1 |   1 |      AA |    EWR |  MIA |      161 |     1085 |   16 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  SFO |      365 |     2586 |   17 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  BOS |       39 |      187 |   12 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  LAX |      345 |     2475 |   19 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  BOS |       35 |      187 |   17 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  ORD |      155 |      740 |   17 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  IAH |      234 |     1417 |   16 |
;;    | 2014 |     1 |   1 |      AA |    JFK |  AUS |      232 |     1521 |   17 |
;;    | 2014 |     1 |   1 |      AA |    EWR |  DFW |      214 |     1372 |   16 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      142 |      733 |    5 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      143 |      733 |    6 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      139 |      733 |    6 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      145 |      733 |    7 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      139 |      733 |    8 |
;;    | 2014 |     1 |   1 |      AA |    LGA |  ORD |      141 |      733 |   10 |

;;
;; # Aggregation
;;

(defn map-v [f coll]
  (reduce-kv (fn [m k v] (assoc m k (f v))) (empty coll) coll))

;; ## How can we get the number of trips corresponding to each origin airport?

;; ans <- flights[, .(.N), by = .(origin)]
;; ans <- flights[, .(.N), by = "origin"]

;; --------- R

(def ans (r '(bra ~R-flights nil (. .N) :by (. origin))))

ans
;; =>    origin     N
;;    1:    JFK 81483
;;    2:    LGA 84433
;;    3:    EWR 87400

;; or

(def ans (r '(bra ~R-flights nil (. .N) :by "origin")))

ans
;; =>    origin     N
;;    1:    JFK 81483
;;    2:    LGA 84433
;;    3:    EWR 87400


;; --------- Clojure

;; TODO: maybe add `group-by-and-aggregate` which returns dataset after group-by and aggregation?

(def ans (->> flights
              (ds/group-by-column "origin")
              (map-v ds/row-count)))

ans
;; => {"EWR" 87400, "LGA" 84433, "JFK" 81483}

;; or (wrong)

(def ans (->> flights
              (ds/group-by-column "origin")
              (map-v (comp vector ds/row-count))
              (ds/name-values-seq->dataset)))

ans
;; => _unnamed [1 3]:
;;    |   EWR |   LGA |   JFK |
;;    |-------+-------+-------|
;;    | 87400 | 84433 | 81483 |

;; or (good)

(defn group-by-columns-and-aggregate
  [gr-colls agg-fns-map ds]
  (->> (ds/group-by identity ds gr-colls)
       (map (fn [[group-idx group-ds]]
              (aggregate group-idx agg-fns-map group-ds)))
       ds/->dataset))

(def ans (group-by-columns-and-aggregate ["origin"] {"N" ds/row-count} flights))

ans
;; => _unnamed [3 2]:
;;    | origin |     N |
;;    |--------+-------|
;;    |    LGA | 84433 |
;;    |    EWR | 87400 |
;;    |    JFK | 81483 |


;; ## How can we calculate the number of trips for each origin airport for carrier code "AA"?

;; ans <- flights[carrier == "AA", .N, by = origin]

;; --------- R

(def ans (r/bra R-flights '(== carrier "AA") '.N :by 'origin))

ans
;; =>    origin     N
;;    1:    JFK 11923
;;    2:    LGA 11730
;;    3:    EWR  2649

;; --------- Clojure

(def ans (->> flights
              (ds/filter #(= "AA" (get % "carrier")))
              (group-by-columns-and-aggregate ["origin"] {"N" ds/row-count})))

ans
;; => _unnamed [3 2]:
;;    | origin |     N |
;;    |--------+-------|
;;    |    LGA | 11730 |
;;    |    EWR |  2649 |
;;    |    JFK | 11923 |

;; ## How can we get the total number of trips for each origin, dest pair for carrier code "AA"?

;; ans <- flights[carrier == "AA", .N, by = .(origin, dest)]

;; --------- R

(def ans (r/bra R-flights '(== carrier "AA") '.N :by '(. origin dest)))

(r.utils/head ans)
;; =>    origin dest    N
;;    1:    JFK  LAX 3387
;;    2:    LGA  PBI  245
;;    3:    EWR  LAX   62
;;    4:    JFK  MIA 1876
;;    5:    JFK  SEA  298
;;    6:    EWR  MIA  848

;; or

(def ans (r/bra R-flights '(== carrier "AA") '.N :by ["origin" "dest"]))

(r.utils/head ans)
;; =>    origin dest    N
;;    1:    JFK  LAX 3387
;;    2:    LGA  PBI  245
;;    3:    EWR  LAX   62
;;    4:    JFK  MIA 1876
;;    5:    JFK  SEA  298
;;    6:    EWR  MIA  848

;; --------- Clojure

(def ans (->> flights
              (ds/filter #(= "AA" (get % "carrier")))
              (group-by-columns-and-aggregate ["origin" "dest"] {"N" ds/row-count})))

(ds/select-rows ans (range 6))
;; => _unnamed [6 3]:
;;    | origin | dest |    N |
;;    |--------+------+------|
;;    |    EWR |  PHX |  121 |
;;    |    LGA |  MIA | 3334 |
;;    |    JFK |  LAX | 3387 |
;;    |    JFK |  IAH |    7 |
;;    |    JFK |  LAS |  595 |
;;    |    JFK |  MCO |  597 |

;; ## How can we get the average arrival and departure delay for each orig,dest pair for each month for carrier code "AA"?

;; ans <- flights[carrier == "AA",
;;                .(mean(arr_delay), mean(dep_delay)),
;;                by = .(origin, dest, month)]

;; --------- R

(def ans (r/bra R-flights '(== carrier "AA") '(. (mean arr_delay)
                                                 (mean dep_delay)) :by '(. origin dest month)))

ans
;; =>      origin dest month         V1         V2
;;      1:    JFK  LAX     1   6.590361 14.2289157
;;      2:    LGA  PBI     1  -7.758621  0.3103448
;;      3:    EWR  LAX     1   1.366667  7.5000000
;;      4:    JFK  MIA     1  15.720670 18.7430168
;;      5:    JFK  SEA     1  14.357143 30.7500000
;;     ---                                        
;;    196:    LGA  MIA    10  -6.251799 -1.4208633
;;    197:    JFK  MIA    10  -1.880184  6.6774194
;;    198:    EWR  PHX    10  -3.032258 -4.2903226
;;    199:    JFK  MCO    10 -10.048387 -1.6129032
;;    200:    JFK  DCA    10  16.483871 15.5161290


;; --------- Clojure

(def ans (->> flights
              (ds/filter #(= "AA" (get % "carrier")))
              (group-by-columns-and-aggregate ["origin" "dest" "month"]
                                              {"V1" #(dfn/mean (% "arr_delay"))
                                               "V2" #(dfn/mean (% "dep_delay"))})))

ans
;; => _unnamed [200 5]:
;;    | origin | dest | month |      V2 |      V1 |
;;    |--------+------+-------+---------+---------|
;;    |    JFK |  SEA |     9 |   16.83 |   8.567 |
;;    |    JFK |  MCO |     2 |   10.15 |   8.453 |
;;    |    LGA |  MIA |     1 |   5.417 |   6.130 |
;;    |    JFK |  LAS |     3 |   6.869 |   13.18 |
;;    |    JFK |  LAS |     1 |   19.15 |   17.69 |
;;    |    LGA |  MIA |     4 |  0.6552 |  -3.828 |
;;    |    JFK |  EGE |     2 |   57.46 |   53.79 |
;;    |    LGA |  DFW |     3 |  -1.285 | -0.2461 |
;;    |    EWR |  DFW |     8 |   22.07 |   16.94 |
;;    |    LGA |  MIA |     6 |   8.467 |  -2.458 |
;;    |    LGA |  PBI |     5 |  -6.857 |  -10.36 |
;;    |    EWR |  PHX |     9 |  -1.667 |  -4.233 |
;;    |    JFK |  SAN |     9 |   12.83 |   18.80 |
;;    |    JFK |  SFO |     3 |   10.03 |   5.586 |
;;    |    JFK |  AUS |     4 | -0.1333 |   4.367 |
;;    |    JFK |  SAN |     8 |   14.19 |   10.74 |
;;    |    LGA |  ORD |     6 |   17.30 |   18.91 |
;;    |    JFK |  SFO |     9 |   6.207 |   7.233 |
;;    |    LGA |  DFW |    10 |   4.553 |   3.500 |
;;    |    JFK |  ORD |     7 |   34.39 |   23.14 |
;;    |    JFK |  SJU |     9 |   11.38 |   1.688 |
;;    |    JFK |  SEA |     7 |   20.55 |   21.97 |
;;    |    JFK |  LAS |    10 |   14.55 |   18.23 |
;;    |    JFK |  ORD |     2 |   41.74 |   34.11 |
;;    |    JFK |  STT |     6 |  0.9667 |  -4.667 |

;; ## So how can we directly order by all the grouping variables?

;; ans <- flights[carrier == "AA",
;;                .(mean(arr_delay), mean(dep_delay)),
;;                keyby = .(origin, dest, month)]

;; --------- R

(def ans (r/bra R-flights '(== carrier "AA") '(. (mean arr_delay)
                                                 (mean dep_delay)) :keyby '(. origin dest month)))

ans
;; =>      origin dest month         V1         V2
;;      1:    EWR  DFW     1   6.427673 10.0125786
;;      2:    EWR  DFW     2  10.536765 11.3455882
;;      3:    EWR  DFW     3  12.865031  8.0797546
;;      4:    EWR  DFW     4  17.792683 12.9207317
;;      5:    EWR  DFW     5  18.487805 18.6829268
;;     ---                                        
;;    196:    LGA  PBI     1  -7.758621  0.3103448
;;    197:    LGA  PBI     2  -7.865385  2.4038462
;;    198:    LGA  PBI     3  -5.754098  3.0327869
;;    199:    LGA  PBI     4 -13.966667 -4.7333333
;;    200:    LGA  PBI     5 -10.357143 -6.8571429

;; --------- Clojure

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

(def ans (->> flights
              (ds/filter #(= "AA" (get % "carrier")))
              (group-by-columns-and-aggregate ["origin" "dest" "month"]
                                              {"V1" #(dfn/mean (% "arr_delay"))
                                               "V2" #(dfn/mean (% "dep_delay"))})
              (sort-by-columns-with-orders ["origin" "dest" "month"])))

ans
;; => _unnamed [200 5]:
;;    | origin | dest | month |     V2 |      V1 |
;;    |--------+------+-------+--------+---------|
;;    |    EWR |  DFW |     1 |  10.01 |   6.428 |
;;    |    EWR |  DFW |     2 |  11.35 |   10.54 |
;;    |    EWR |  DFW |     3 |  8.080 |   12.87 |
;;    |    EWR |  DFW |     4 |  12.92 |   17.79 |
;;    |    EWR |  DFW |     5 |  18.68 |   18.49 |
;;    |    EWR |  DFW |     6 |  38.74 |   37.01 |
;;    |    EWR |  DFW |     7 |  21.15 |   20.25 |
;;    |    EWR |  DFW |     8 |  22.07 |   16.94 |
;;    |    EWR |  DFW |     9 |  13.06 |   5.865 |
;;    |    EWR |  DFW |    10 |  18.89 |   18.81 |
;;    |    EWR |  LAX |     1 |  7.500 |   1.367 |
;;    |    EWR |  LAX |     2 |  4.111 |   10.33 |
;;    |    EWR |  LAX |     3 | -6.800 |  -4.400 |
;;    |    EWR |  MIA |     1 |  12.12 |   11.01 |
;;    |    EWR |  MIA |     2 |  4.756 |   1.564 |
;;    |    EWR |  MIA |     3 | 0.4444 |  -4.111 |
;;    |    EWR |  MIA |     4 |  6.433 |   3.189 |
;;    |    EWR |  MIA |     5 |  6.344 |  -2.538 |
;;    |    EWR |  MIA |     6 |  16.20 |   7.307 |
;;    |    EWR |  MIA |     7 |  26.35 |   25.22 |
;;    |    EWR |  MIA |     8 | 0.8462 |  -6.125 |
;;    |    EWR |  MIA |     9 | 0.3594 | -0.9063 |
;;    |    EWR |  MIA |    10 | -3.787 |  -4.475 |
;;    |    EWR |  PHX |     7 | 0.2759 |  -5.103 |
;;    |    EWR |  PHX |     8 |  6.226 |   3.548 |

;; ## How can we order ans using the columns origin in ascending order, and dest in descending order?

;; ans <- flights[carrier == "AA", .N, by = .(origin, dest)]
;; ans <- ans[order(origin, -dest)]
;;
;; ans <- flights[carrier == "AA", .N, by = .(origin, dest)][order(origin, -dest)]

;; --------- R

(def ans (r/bra R-flights '(== carrier "AA") '.N :by '(. origin dest)))
(def ans (r/bra ans '(order origin (- dest))))

(r.utils/head ans)
;; =>    origin dest    N
;;    1:    EWR  PHX  121
;;    2:    EWR  MIA  848
;;    3:    EWR  LAX   62
;;    4:    EWR  DFW 1618
;;    5:    JFK  STT  229
;;    6:    JFK  SJU  690

;; or

(def ans (-> (r/bra R-flights '(== carrier "AA") '.N :by '(. origin dest))
             (r/bra '(order origin (- dest)))))

(r.utils/head ans)
;; =>    origin dest    N
;;    1:    EWR  PHX  121
;;    2:    EWR  MIA  848
;;    3:    EWR  LAX   62
;;    4:    EWR  DFW 1618
;;    5:    JFK  STT  229
;;    6:    JFK  SJU  690


;; --------- Clojure

(def ans (->> flights
              (ds/filter #(= "AA" (get % "carrier")))
              (group-by-columns-and-aggregate ["origin" "dest"]
                                              {"N" ds/row-count})
              (sort-by-columns-with-orders ["origin" "dest"] [:asc :desc])))

(ds/select-rows ans (range 6))
;; => _unnamed [6 3]:
;;    | origin | dest |    N |
;;    |--------+------+------|
;;    |    EWR |  PHX |  121 |
;;    |    EWR |  MIA |  848 |
;;    |    EWR |  LAX |   62 |
;;    |    EWR |  DFW | 1618 |
;;    |    JFK |  STT |  229 |
;;    |    JFK |  SJU |  690 |

;; ## Can by accept expressions as well or does it just take columns

;; ans <- flights[, .N, .(dep_delay>0, arr_delay>0)]

;; --------- R

(def ans (r '(bra ~R-flights nil .N (. (> dep_delay 0)
                                       (> arr_delay 0)))))

ans
;; =>    dep_delay arr_delay      N
;;    1:      TRUE      TRUE  72836
;;    2:     FALSE      TRUE  34583
;;    3:     FALSE     FALSE 119304
;;    4:      TRUE     FALSE  26593

;; --------- Clojure

;; TODO: group by inline transformation on several columns
;; changed to `new-column`
(def ans (->> (-> flights
                  (ds/new-column :pos_dep_delay (dfn/> (flights "dep_delay") 0))
                  (ds/new-column :pos_arr_delay (dfn/> (flights "arr_delay") 0)))
              (group-by-columns-and-aggregate [:pos_dep_delay :pos_arr_delay]
                                              {"N" ds/row-count})))

ans
;; => _unnamed [4 3]:
;;    | :pos_dep_delay | :pos_arr_delay |      N |
;;    |----------------+----------------+--------|
;;    |          false |           true |  34583 |
;;    |           true |          false |  26593 |
;;    |          false |          false | 119304 |
;;    |           true |           true |  72836 |

;; ## Do we have to compute mean() for each column individually?

;; DT
;; DT[, print(.SD), by = ID]
;; DT[, lapply(.SD, mean), by = ID]

;; --------- R

R-DT
;; =>    ID a  b  c
;;    1:  b 1  7 13
;;    2:  b 2  8 14
;;    3:  b 3  9 15
;;    4:  c 4 10 16
;;    5:  c 5 11 17
;;    6:  a 6 12 18

(r '(bra ~R-DT nil (print .SD) :by ID))
;; => Empty data.table (0 rows and 1 cols): ID

;; prints:
;; a b  c
;; 1: 1 7 13
;; 2: 2 8 14
;; 3: 3 9 15
;; a  b  c
;; 1: 4 10 16
;; 2: 5 11 17
;; a  b  c
;; 1: 6 12 18

(r '(bra ~R-DT nil (lapply .SD mean) :by ID))
;; =>    ID   a    b    c
;;    1:  b 2.0  8.0 14.0
;;    2:  c 4.5 10.5 16.5
;;    3:  a 6.0 12.0 18.0

;; --------- Clojure

DT
;; => _unnamed [6 4]:
;;    | :ID | :a | :b | :c |
;;    |-----+----+----+----|
;;    |   b |  1 |  7 | 13 |
;;    |   b |  2 |  8 | 14 |
;;    |   b |  3 |  9 | 15 |
;;    |   c |  4 | 10 | 16 |
;;    |   c |  5 | 11 | 17 |
;;    |   a |  6 | 12 | 18 |

(ds/group-by-column :ID DT)

;; => {"a" a [1 4]:
;;    | :ID | :a | :b | :c |
;;    |-----+----+----+----|
;;    |   a |  6 | 12 | 18 |
;;    , "b" b [3 4]:
;;    | :ID | :a | :b | :c |
;;    |-----+----+----+----|
;;    |   b |  1 |  7 | 13 |
;;    |   b |  2 |  8 | 14 |
;;    |   b |  3 |  9 | 15 |
;;    , "c" c [2 4]:
;;    | :ID | :a | :b | :c |
;;    |-----+----+----+----|
;;    |   c |  4 | 10 | 16 |
;;    |   c |  5 | 11 | 17 |
;;    }

(group-by-columns-and-aggregate [:ID]
                                (into {} (map (fn [col]
                                                [col #(dfn/mean (% col))])
                                              (rest (ds/column-names DT)))) DT) 
;; => _unnamed [3 4]:
;;    | :ID |    :c |    :b |    :a |
;;    |-----+-------+-------+-------|
;;    |   a | 18.00 | 12.00 | 6.000 |
;;    |   b | 14.00 | 8.000 | 2.000 |
;;    |   c | 16.50 | 10.50 | 4.500 |

;; ## How can we specify just the columns we would like to compute the mean() on?

;; flights[carrier == "AA",                       ## Only on trips with carrier "AA"
;;         lapply(.SD, mean),                     ## compute the mean
;;         by = .(origin, dest, month),           ## for every 'origin,dest,month'
;;         .SDcols = c("arr_delay", "dep_delay")] ## for just those specified in .SDcols

;; --------- R

(r/bra R-flights
       '(== carrier "AA")
       '(lapply .SD mean)
       :by '(. origin dest month)
       :.SDcols ["arr_delay", "dep_delay"])

;; =>      origin dest month  arr_delay  dep_delay
;;      1:    JFK  LAX     1   6.590361 14.2289157
;;      2:    LGA  PBI     1  -7.758621  0.3103448
;;      3:    EWR  LAX     1   1.366667  7.5000000
;;      4:    JFK  MIA     1  15.720670 18.7430168
;;      5:    JFK  SEA     1  14.357143 30.7500000
;;     ---                                        
;;    196:    LGA  MIA    10  -6.251799 -1.4208633
;;    197:    JFK  MIA    10  -1.880184  6.6774194
;;    198:    EWR  PHX    10  -3.032258 -4.2903226
;;    199:    JFK  MCO    10 -10.048387 -1.6129032
;;    200:    JFK  DCA    10  16.483871 15.5161290

;; --------- Clojure

(->> flights
     (ds/filter #(= (get % "carrier") "AA"))
     (group-by-columns-and-aggregate ["origin", "dest", "month"]
                                     (into {} (map (fn [col]
                                                     [col #(dfn/mean (% col))])
                                                   ["arr_delay" "dep_delay"]))))

;; => _unnamed [200 5]:
;;    | origin | dest | month | dep_delay | arr_delay |
;;    |--------+------+-------+-----------+-----------|
;;    |    JFK |  SEA |     9 |     16.83 |     8.567 |
;;    |    JFK |  MCO |     2 |     10.15 |     8.453 |
;;    |    LGA |  MIA |     1 |     5.417 |     6.130 |
;;    |    JFK |  LAS |     3 |     6.869 |     13.18 |
;;    |    JFK |  LAS |     1 |     19.15 |     17.69 |
;;    |    LGA |  MIA |     4 |    0.6552 |    -3.828 |
;;    |    JFK |  EGE |     2 |     57.46 |     53.79 |
;;    |    LGA |  DFW |     3 |    -1.285 |   -0.2461 |
;;    |    EWR |  DFW |     8 |     22.07 |     16.94 |
;;    |    LGA |  MIA |     6 |     8.467 |    -2.458 |
;;    |    LGA |  PBI |     5 |    -6.857 |    -10.36 |
;;    |    EWR |  PHX |     9 |    -1.667 |    -4.233 |
;;    |    JFK |  SAN |     9 |     12.83 |     18.80 |
;;    |    JFK |  SFO |     3 |     10.03 |     5.586 |
;;    |    JFK |  AUS |     4 |   -0.1333 |     4.367 |
;;    |    JFK |  SAN |     8 |     14.19 |     10.74 |
;;    |    LGA |  ORD |     6 |     17.30 |     18.91 |
;;    |    JFK |  SFO |     9 |     6.207 |     7.233 |
;;    |    LGA |  DFW |    10 |     4.553 |     3.500 |
;;    |    JFK |  ORD |     7 |     34.39 |     23.14 |
;;    |    JFK |  SJU |     9 |     11.38 |     1.688 |
;;    |    JFK |  SEA |     7 |     20.55 |     21.97 |
;;    |    JFK |  LAS |    10 |     14.55 |     18.23 |
;;    |    JFK |  ORD |     2 |     41.74 |     34.11 |
;;    |    JFK |  STT |     6 |    0.9667 |    -4.667 |

;; ## How can we return the first two rows for each month?

;; ans <- flights[, head(.SD, 2), by = month]

;; --------- R

(def ans (r '(bra ~R-flights nil (head .SD 2) :by month)))

(r.utils/head ans)
;; =>    month year day dep_delay arr_delay carrier origin dest air_time distance hour
;;    1:     1 2014   1        14        13      AA    JFK  LAX      359     2475    9
;;    2:     1 2014   1        -3        13      AA    JFK  LAX      363     2475   11
;;    3:     2 2014   1        -1         1      AA    JFK  LAX      358     2475    8
;;    4:     2 2014   1        -5         3      AA    JFK  LAX      358     2475   11
;;    5:     3 2014   1       -11        36      AA    JFK  LAX      375     2475    8
;;    6:     3 2014   1        -3        14      AA    JFK  LAX      368     2475   11

;; --------- Clojure

(def ans (->> flights
              (ds/group-by-column "month")
              (vals)
              (map #(ds/select-rows % [1 2]))
              (reduce ds/concat)))

(ds/select-rows ans (range 6))
;; => null [6 11]:
;;    | dep_delay | origin | air_time | hour | arr_delay | dest | distance | year | month | day | carrier |
;;    |-----------+--------+----------+------+-----------+------+----------+------+-------+-----+---------|
;;    |        -6 |    EWR |      320 |    9 |       -14 |  LAX |     2454 | 2014 |     7 |   1 |      VX |
;;    |        -3 |    EWR |      326 |   12 |       -14 |  LAX |     2454 | 2014 |     7 |   1 |      VX |
;;    |        -3 |    JFK |      363 |   11 |        13 |  LAX |     2475 | 2014 |     1 |   1 |      AA |
;;    |         2 |    JFK |      351 |   19 |         9 |  LAX |     2475 | 2014 |     1 |   1 |      AA |
;;    |        -8 |    LGA |       71 |   18 |       -11 |  RDU |      431 | 2014 |     4 |   1 |      MQ |
;;    |        -4 |    LGA |      113 |    8 |        -2 |  BNA |      764 | 2014 |     4 |   1 |      MQ |

;; ## How can we concatenate columns a and b for each group in ID?

;; DT[, .(val = c(a,b)), by = ID]

;; --------- R

(r '(bra ~R-DT nil (. :val [a b]) :by ID))
;; =>     ID val
;;     1:  b   1
;;     2:  b   2
;;     3:  b   3
;;     4:  b   7
;;     5:  b   8
;;     6:  b   9
;;     7:  c   4
;;     8:  c   5
;;     9:  c  10
;;    10:  c  11
;;    11:  a   6
;;    12:  a  12

;; --------- Clojure

;; TODO: reshape?
(ds/concat (-> (ds/select-columns DT [:ID :a])
               (ds/rename-columns {:a :val}))
           (-> (ds/select-columns DT [:ID :b])
               (ds/rename-columns {:b :val})))
;; => null [12 2]:
;;    | :ID | :val |
;;    |-----+------|
;;    |   b |    1 |
;;    |   b |    2 |
;;    |   b |    3 |
;;    |   c |    4 |
;;    |   c |    5 |
;;    |   a |    6 |
;;    |   b |    7 |
;;    |   b |    8 |
;;    |   b |    9 |
;;    |   c |   10 |
;;    |   c |   11 |
;;    |   a |   12 |

;; ## What if we would like to have all the values of column a and b concatenated, but returned as a list column?

;; --------- R

;; DT[, .(val = list(c(a,b))), by = ID]

(r '(bra ~R-DT nil (. :val (list [a b])) :by ID))
;; =>    ID         val
;;    1:  b 1,2,3,7,8,9
;;    2:  c  4, 5,10,11
;;    3:  a        6,12

;; --------- Clojure

(group-by-columns-and-aggregate [:ID]
                                {:val #(concat (% :a) (% :b))}
                                DT)
;; => _unnamed [3 2]:
;;    | :ID |                          :val |
;;    |-----+-------------------------------|
;;    |   a |      clojure.lang.LazySeq@487 |
;;    |   b | clojure.lang.LazySeq@36b8bb47 |
;;    |   c |    clojure.lang.LazySeq@ffd03 |

;; or

;; TODO: printing should realize sequences
(group-by-columns-and-aggregate [:ID]
                                {:val #(vec (concat (seq (% :a)) (seq (% :b))))}
                                DT)
;; => _unnamed [3 2]:
;;    | :ID |          :val |
;;    |-----+---------------|
;;    |   a |        [6 12] |
;;    |   b | [1 2 3 7 8 9] |
;;    |   c |   [4 5 10 11] |

