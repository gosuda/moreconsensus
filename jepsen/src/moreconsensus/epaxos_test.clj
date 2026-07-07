(ns moreconsensus.epaxos-test
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]))

(defn endpoint [test node]
  (let [node (str/replace (str node) #"/$" "")]
    (cond
      (str/starts-with? node "http://") node
      (str/starts-with? node "https://") node
      (str/includes? node ":") (str "http://" node)
      :else (str "http://" node ":" (or (:http-port test) 8080)))))

(defrecord KVClient [node]
  client/Client
  (open! [this test node] (assoc this :node node))
  (setup! [this test]
    (info "using EPaxos KV endpoint" (endpoint test node)))
  (invoke! [this test op]
    (let [base (endpoint test node)
          k "k0"]
      (try
        (case (:f op)
          :write
          (let [resp (http/put (str base "/kv/" k)
                               {:body (pr-str (:value op))
                                :throw-exceptions false})]
            (if (contains? #{200 201 202 204} (:status resp))
              (assoc op :type :ok)
              (assoc op :type :fail :error (:status resp))))
          :read
          (let [resp (http/get (str base "/kv/" k)
                               {:throw-exceptions false})]
            (if (= 200 (:status resp))
              (assoc op :type :ok :value (edn/read-string (:body resp)))
              (assoc op :type :fail :error (:status resp))))
          (assoc op :type :fail :error :unknown-operation))
        (catch Exception e
          (warn e "operation failed")
          (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [this test])
  (close! [this test]))

(defn workload []
  {:client (->KVClient nil)
   :generator (gen/clients
               (gen/limit 60
                          (gen/mix [(map (fn [x] {:type :invoke :f :write :value x}) (range))
                                    (gen/repeat {:type :invoke :f :read :value nil})])))
   :checker (checker/compose {:linearizable (checker/linearizable {:model (model/register)})
                              :timeline (timeline/html)})})

(defn moreconsensus-test [opts]
  (merge tests/noop-test
         opts
         (workload)
         {:name "moreconsensus-epaxos-kv"}))

(defn -main [& args]
  (cli/run! (cli/single-test-cmd {:test-fn moreconsensus-test}) args))
