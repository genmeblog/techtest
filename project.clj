(defproject techtest "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins [[cider/cider-nrepl "0.25.0-alpha1"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [techascent/tech.ml.dataset "2.0-beta-40" :exclusions [org.clojure/tools.logging]]
                 [scicloj/clojisr "1.0.0-BETA11-SNAPSHOT"]
                 [generateme/fastmath "1.5.3-SNAPSHOT"]])
