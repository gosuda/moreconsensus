(defproject moreconsensus-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen workload harness for moreconsensus EPaxos"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [org.clojure/data.json "2.5.2"]
                 [clj-http "3.13.1"]
                 [jepsen "0.3.11"]]
  :main moreconsensus.epaxos-test)
