(ns moreconsensus.epaxos-test
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
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

(def register-ops #{:read :write})

(def txn-key-groups
  [{:group :tx-a :keys ["tx-a0" "tx-a1"]}
   {:group :tx-b :keys ["tx-b0" "tx-b1" "tx-b2"]}
   {:group :tx-c :keys ["tx-c0" "tx-c1"]}])

(def txn-group-ids (mapv :group txn-key-groups))
(def txn-keys (vec (mapcat :keys txn-key-groups)))
(def txn-keys-by-group (into {} (map (juxt :group :keys)) txn-key-groups))
(def txn-scan-barrier (str/join "," txn-keys))

(defn ok-status? [status]
  (contains? #{200 201 202 204} status))

(defn txn-group-for [n]
  (nth txn-group-ids (mod n (count txn-group-ids))))

(defn txn-body [group value]
  (let [keys (or (get txn-keys-by-group group)
                 (throw (ex-info "unknown transaction group" {:group group})))]
    (json/write-str
     (mapv (fn [k] {"key" k "value" (pr-str value)}) keys))))

(defn parse-rows [body]
  (json/read-str body :key-fn keyword))

(defn row-values [rows]
  (into {} (map (fn [row]
                  [(:key row) (edn/read-string (:value row))])
                rows)))

(defn scan-map
  ([base prefix] (scan-map base prefix nil))
  ([base prefix barrier]
   (try
     (let [params (cond-> {"prefix" prefix}
                    barrier (assoc "barrier" barrier))
           resp (http/get (str base "/scan")
                          {:query-params params
                           :throw-exceptions false})]
       (if (= 200 (:status resp))
         [:ok (row-values (parse-rows (:body resp)))]
         [:fail (:status resp)]))
     (catch Exception e
       [:fail (.getMessage e)]))))

(defn scan-values [base]
  (let [[status values] (scan-map base "tx-" txn-scan-barrier)]
    (if (= :ok status)
      [:ok (into {} (map (fn [{:keys [group keys]}]
                           [group (mapv #(get values %) keys)])
                         txn-key-groups))]
      [:fail values])))


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
            (if (ok-status? (:status resp))
              (assoc op :type :ok)
              (assoc op :type :fail :error (:status resp))))
          :read
          (let [resp (http/get (str base "/kv/" k)
                               {:throw-exceptions false})]
            (if (= 200 (:status resp))
              (assoc op :type :ok :value (edn/read-string (:body resp)))
              (assoc op :type :fail :error (:status resp))))
          :txn-write
          (let [{:keys [group value]} (:value op)
                resp (http/post (str base "/txn")
                                {:body (txn-body group value)
                                 :content-type :json
                                 :throw-exceptions false})]
            (if (ok-status? (:status resp))
              (assoc op :type :ok)
              (assoc op :type :fail :error (:status resp))))
          :txn-read
          (let [[status values] (scan-values base)]
            (if (= :ok status)
              (assoc op :type :ok :value values)
              (assoc op :type :fail :error values)))
          (assoc op :type :fail :error :unknown-operation))
        (catch Exception e
          (warn e "operation failed")
          (assoc op :type :fail :error (.getMessage e))))))
  (teardown! [this test])
  (close! [this test]))

(defn register-linearizable-checker []
  (let [linear (checker/linearizable {:model (model/register)})]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check linear test (filterv #(contains? register-ops (:f %)) history) opts)))))

(defn inconsistent-txn-groups [groups]
  (into {} (keep (fn [[group values]]
                   (when-not (apply = values)
                     [group values]))
                 groups)))

(defn txn-atomic-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [reads (filter #(and (= :ok (:type %)) (= :txn-read (:f %))) history)
            bad (vec (keep (fn [op]
                             (let [groups (inconsistent-txn-groups (:value op))]
                               (when (seq groups)
                                 (assoc op :bad-groups groups))))
                           reads))]
        {:valid? (empty? bad)
         :checked (count reads)
         :bad-count (count bad)
         :bad (take 5 bad)}))))

(defn workload []
  {:client (->KVClient nil)
   :generator (gen/clients
               (gen/limit 90
                          (gen/mix [(map (fn [x] {:type :invoke :f :write :value x}) (range))
                                    (gen/repeat {:type :invoke :f :read :value nil})
                                    (map (fn [x] {:type :invoke :f :txn-write :value {:group (txn-group-for x) :value x}}) (range))
                                    (gen/repeat {:type :invoke :f :txn-read :value nil})])))
   :checker (checker/compose {:linearizable (register-linearizable-checker)
                              :txn-atomic (txn-atomic-checker)
                              :timeline (timeline/html)})})

(defn moreconsensus-test [opts]
  (merge tests/noop-test
         opts
         (workload)
         {:name "moreconsensus-epaxos-kv"}))

(defn -main [& args]
  (cli/run! (cli/single-test-cmd {:test-fn moreconsensus-test}) args))
