(defproject moreconsensus-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen workload harness for moreconsensus EPaxos"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/data.json "2.5.2"]
                 [clj-http "3.13.0"]
                 [jepsen "0.3.7"]]
  :main moreconsensus.epaxos-test)
