(defproject jepsen.barka "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Barka distributed log"
  :license {:name "EPL-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.6"]
                 [cheshire "5.12.0"]]
  :main jepsen.barka.core
  :profiles {:uberjar {:aot :all}})
