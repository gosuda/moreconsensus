(ns moreconsensus.epaxos-test-test
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [moreconsensus.epaxos-test :as epaxos]))

(defn fault-env-from [values]
  (fn [suffix]
    (get values suffix)))

(def client-operation-fs
  #{:read :write :delete
    :scan-write :scan-delete :scan-forward :scan-reverse
    :txn-write :txn-delete :txn-read})

(defn generated-ops-for [generator process n]
  (let [test {:concurrency 1}
        ctx (gen/context test)]
    (loop [generator (gen/on #{process} generator)
           ops []]
      (if (= n (count ops))
        ops
        (if-let [[op generator'] (gen/op generator test ctx)]
          (recur generator' (conj ops op))
          ops)))))

(defn fault-ops [ops]
  (->> ops
       (keep (fn [op]
               (when (:f op)
                 (select-keys op [:type :process :f :value]))))
       vec))

(deftest local-fault-config-requires-enabled-local-fault
  (let [base-env {"BIN" "/tmp/kvnode"
                  "DATA_DIR" "/tmp/data"
                  "PEERS" "127.0.0.1:9001,127.0.0.1:9002"
                  "PID_DIR" "/tmp/pids"
                  "BASE_PORT" "9000"}
        nodes ["127.0.0.1:9001" "127.0.0.1:9002"]]
    (doseq [faults [nil "" "crash" "Restart" " restart "]]
      (testing (str "fault selector " (pr-str faults))
        (with-redefs [epaxos/fault-env (fault-env-from (assoc base-env "FAULTS" faults))]
          (is (nil? (epaxos/local-fault-config {:nodes nodes}))))))))

(deftest local-fault-config-uses-provided-restart-env
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        env {"FAULTS" "restart"
             "BIN" "/opt/moreconsensus/kvnode"
             "DATA_DIR" "/var/lib/moreconsensus"
             "PEERS" "127.0.0.1:9101,127.0.0.1:9102"
             "PID_DIR" "/var/run/moreconsensus"
             "BASE_PORT" "9100"}]
    (with-redefs [epaxos/fault-env (fault-env-from env)]
      (is (= {:faults "restart"
              :bin "/opt/moreconsensus/kvnode"
              :data-dir "/var/lib/moreconsensus"
              :peers "127.0.0.1:9101,127.0.0.1:9102"
              :pid-dir "/var/run/moreconsensus"
              :base-port 9100
              :nodes nodes}
             (epaxos/local-fault-config {:nodes nodes}))))))

(deftest local-fault-config-uses-provided-transport-env
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        env {"FAULTS" "transport"
             "BIN" "/opt/moreconsensus/kvnode"
             "DATA_DIR" "/var/lib/moreconsensus"
             "PEERS" "127.0.0.1:9101,127.0.0.1:9102"
             "PID_DIR" "/var/run/moreconsensus"
             "BASE_PORT" "9100"}]
    (with-redefs [epaxos/fault-env (fault-env-from env)]
      (is (= {:faults "transport"
              :base-port 9100
              :nodes nodes}
             (epaxos/local-fault-config {:nodes nodes}))))))

(deftest local-fault-config-uses-provided-storage-env
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        env {"FAULTS" "storage"
             "BASE_PORT" "9100"}]
    (with-redefs [epaxos/fault-env (fault-env-from env)]
      (is (= {:faults "storage"
              :base-port 9100
              :nodes nodes}
             (epaxos/local-fault-config {:nodes nodes}))))))

(deftest local-restart-generator-emits-node-restarts-in-order
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        ops (take 9 (epaxos/local-restart-generator nodes))]
    (is (= [{:type :info :f :kill-node :value "127.0.0.1:9101"}
            {:type :info :f :restart-node :value "127.0.0.1:9101"}
            {:type :info :f :kill-node :value "127.0.0.1:9102"}
            {:type :info :f :restart-node :value "127.0.0.1:9102"}]
           (fault-ops ops)))
    (is (= 8 (count ops)))))

(deftest local-transport-generator-emits-isolate-and-heal-in-order
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        ops (take 9 (epaxos/local-transport-generator nodes))]
    (is (= [{:type :info :f :isolate-node :value "127.0.0.1:9101"}
            {:type :info :f :heal-node :value "127.0.0.1:9101"}
            {:type :info :f :isolate-node :value "127.0.0.1:9102"}
            {:type :info :f :heal-node :value "127.0.0.1:9102"}]
           (fault-ops ops)))
    (is (= 8 (count ops)))))

(deftest local-storage-generator-emits-fail-and-heal-in-order
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        ops (take 9 (epaxos/local-storage-generator nodes))]
    (is (= [{:type :info :f :fail-storage :value "127.0.0.1:9101"}
            {:type :info :f :heal-storage :value "127.0.0.1:9101"}
            {:type :info :f :fail-storage :value "127.0.0.1:9102"}
            {:type :info :f :heal-storage :value "127.0.0.1:9102"}]
           (fault-ops ops)))
    (is (= 8 (count ops)))))

(deftest workload-routes-client-and-nemesis-generators
  (testing "restart faults keep the node restart nemesis operations"
    (let [fault-cfg {:faults "restart"
                     :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
          generator (:generator (epaxos/workload fault-cfg))
          [client-op] (generated-ops-for generator 0 1)
          nemesis-faults (fault-ops (take 5 (epaxos/local-fault-generator fault-cfg)))]
      (is (= :invoke (:type client-op)))
      (is (= 0 (:process client-op)))
      (is (contains? client-operation-fs (:f client-op)))
      (is (= [{:type :info
               :f :kill-node
               :value "127.0.0.1:9101"}
              {:type :info
               :f :restart-node
               :value "127.0.0.1:9101"}]
             nemesis-faults))))
  (testing "transport faults select the isolate and heal nemesis operations"
    (let [fault-cfg {:faults "transport"
                     :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
          generator (:generator (epaxos/workload fault-cfg))
          [client-op] (generated-ops-for generator 0 1)
          nemesis-faults (fault-ops (take 5 (epaxos/local-fault-generator fault-cfg)))]
      (is (= :invoke (:type client-op)))
      (is (= 0 (:process client-op)))
      (is (contains? client-operation-fs (:f client-op)))
      (is (= [{:type :info
               :f :isolate-node
               :value "127.0.0.1:9101"}
              {:type :info
               :f :heal-node
               :value "127.0.0.1:9101"}]
             nemesis-faults)))))

(deftest workload-routes-storage-faults-to-storage-generator
  (let [fault-cfg {:faults "storage"
                   :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
        generator (:generator (epaxos/workload fault-cfg))
        [client-op] (generated-ops-for generator 0 1)
        nemesis-faults (fault-ops (take 5 (epaxos/local-fault-generator fault-cfg)))]
    (is (= :invoke (:type client-op)))
    (is (= 0 (:process client-op)))
    (is (contains? client-operation-fs (:f client-op)))
    (is (= [{:type :info
             :f :fail-storage
             :value "127.0.0.1:9101"}
            {:type :info
             :f :heal-storage
             :value "127.0.0.1:9101"}]
           nemesis-faults))))

(deftest local-restart-nemesis-stops-and-starts-selected-node
  (let [cfg {:faults "restart"
             :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
        calls (atom [])]
    (with-redefs [epaxos/stop-node! (fn [seen-cfg node]
                                      (swap! calls conj [:stop seen-cfg node])
                                      {:node node :action :stopped})
                  epaxos/start-node! (fn [seen-cfg node]
                                       (swap! calls conj [:start seen-cfg node])
                                       {:node node :action :started})]
      (let [nemesis (epaxos/local-fault-nemesis cfg)
            kill-op (nemesis/invoke! nemesis {}
                                     {:type :invoke
                                      :f :kill-node
                                      :value "127.0.0.1:9101"})
            restart-op (nemesis/invoke! nemesis {}
                                        {:type :invoke
                                         :f :restart-node
                                         :value "127.0.0.1:9102"})]
        (is (= [[:stop cfg "127.0.0.1:9101"]
                [:start cfg "127.0.0.1:9102"]]
               @calls))
        (is (= {:type :info
                :f :kill-node
                :value {:node "127.0.0.1:9101" :action :stopped}}
               (select-keys kill-op [:type :f :value])))
        (is (= {:type :info
                :f :restart-node
                :value {:node "127.0.0.1:9102" :action :started}}
               (select-keys restart-op [:type :f :value])))))))

(deftest local-transport-nemesis-issues-control-requests-for-source-and-destination-pairs
  (let [cfg {:faults "transport"
             :base-port 9100
             :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
        calls (atom [])]
    (with-redefs [epaxos/transport-fault-request! (fn [node from to drop?]
                                                   (swap! calls conj [node from to drop?])
                                                   {:node node :from from :to to :drop drop? :status 204})]
      (let [nemesis (epaxos/local-fault-nemesis cfg)
            isolate-op (nemesis/invoke! nemesis {}
                                        {:type :invoke
                                         :f :isolate-node
                                         :value "127.0.0.1:9101"})
            heal-op (nemesis/invoke! nemesis {}
                                     {:type :invoke
                                      :f :heal-node
                                      :value "127.0.0.1:9101"})]
        (is (= [["127.0.0.1:9101" 1 2 true]
                ["127.0.0.1:9101" 2 1 true]
                ["127.0.0.1:9102" 1 2 true]
                ["127.0.0.1:9102" 2 1 true]
                ["127.0.0.1:9101" 1 2 false]
                ["127.0.0.1:9101" 2 1 false]
                ["127.0.0.1:9102" 1 2 false]
                ["127.0.0.1:9102" 2 1 false]]
               @calls))
        (is (= {:type :info
                :f :isolate-node
                :value [{:node "127.0.0.1:9101" :from 1 :to 2 :drop true :status 204}
                        {:node "127.0.0.1:9101" :from 2 :to 1 :drop true :status 204}
                        {:node "127.0.0.1:9102" :from 1 :to 2 :drop true :status 204}
                        {:node "127.0.0.1:9102" :from 2 :to 1 :drop true :status 204}]}
               (select-keys isolate-op [:type :f :value])))
        (is (= {:type :info
                :f :heal-node
                :value [{:node "127.0.0.1:9101" :from 1 :to 2 :drop false :status 204}
                        {:node "127.0.0.1:9101" :from 2 :to 1 :drop false :status 204}
                        {:node "127.0.0.1:9102" :from 1 :to 2 :drop false :status 204}
                        {:node "127.0.0.1:9102" :from 2 :to 1 :drop false :status 204}]}
               (select-keys heal-op [:type :f :value])))))))


(deftest local-storage-nemesis-posts-selected-faults-and-heals-teardown
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        cfg {:faults "storage"
             :base-port 9100
             :nodes nodes}
        requests (atom [])]
    (with-redefs [http/post (fn [url opts]
                              (swap! requests conj [url opts])
                              {:status 204})]
      (let [nemesis (epaxos/local-fault-nemesis cfg)
            fail-op (nemesis/invoke! nemesis {}
                                     {:type :invoke
                                      :f :fail-storage
                                      :value "127.0.0.1:9101"})
            heal-op (nemesis/invoke! nemesis {}
                                     {:type :invoke
                                      :f :heal-storage
                                      :value "127.0.0.1:9101"})]
        (nemesis/teardown! nemesis {:nodes ["127.0.0.1:9999"]})
        (is (= {:type :info
                :f :fail-storage
                :value {:node "127.0.0.1:9101" :fail true :status 204}}
               (select-keys fail-op [:type :f :value])))
        (is (= {:type :info
                :f :heal-storage
                :value {:node "127.0.0.1:9101" :fail false :status 204}}
               (select-keys heal-op [:type :f :value])))
        (is (= [["http://127.0.0.1:9101/faults/storage"
                 {:body (json/write-str {"fail" true})
                  :content-type :json
                  :throw-exceptions false}]
                ["http://127.0.0.1:9101/faults/storage"
                 {:body (json/write-str {"fail" false})
                  :content-type :json
                  :throw-exceptions false}]
                ["http://127.0.0.1:9101/faults/storage"
                 {:body (json/write-str {"fail" false})
                  :content-type :json
                  :throw-exceptions false}]
                ["http://127.0.0.1:9102/faults/storage"
                 {:body (json/write-str {"fail" false})
                  :content-type :json
                  :throw-exceptions false}]]
               @requests))))))

(deftest local-restart-nemesis-surfaces-invoke-errors
  (let [cfg {:nodes ["127.0.0.1:9101"]}]
    (with-redefs [epaxos/stop-node! (fn [_ _]
                                      (throw (ex-info "cannot stop selected node" {})))]
      (let [nemesis (epaxos/local-restart-nemesis cfg)
            op (nemesis/invoke! nemesis {}
                                {:type :invoke
                                 :f :kill-node
                                 :value "127.0.0.1:9101"})]
        (is (= {:type :info
                :f :kill-node
                :value {:error "cannot stop selected node"}}
               (select-keys op [:type :f :value])))))))

(deftest txn-body-encodes-selected-group-as-json
  (testing "writes one EDN value to every key in the chosen transaction group"
    (let [value {:quoted "a\"b" :items [:x 1]}
          rows (json/read-str (epaxos/txn-body :tx-b value) :key-fn keyword)]
      (is (= (mapv (fn [k] {:key k :value (pr-str value)})
                   (get epaxos/txn-keys-by-group :tx-b))
             rows)))))

(deftest txn-delete-body-encodes-selected-group-as-json
  (testing "deletes every key in the chosen transaction group"
    (let [rows (json/read-str (epaxos/txn-delete-body :tx-b) :key-fn keyword)]
      (is (= (mapv (fn [k] {:key k :delete true})
                   (get epaxos/txn-keys-by-group :tx-b))
             rows)))))

(deftest scan-values-returns-grouped-vectors-with-full-key-barrier
  (testing "reads all transaction keys and reports values grouped by independent transaction group"
    (let [flat-values (zipmap epaxos/txn-keys (range))]
      (with-redefs [epaxos/scan-map (fn [_ prefix barrier]
                                      (if (and (= "tx-" prefix)
                                               (= epaxos/txn-scan-barrier barrier))
                                        [:ok flat-values]
                                        [:fail {:prefix prefix :barrier barrier}]))]
        (is (= [:ok (into {} (map (fn [{:keys [group keys]}]
                                    [group (mapv flat-values keys)])
                                  epaxos/txn-key-groups))]
               (epaxos/scan-values "http://node"))))))
  (testing "missing transaction keys are reported as fully deleted grouped reads"
    (with-redefs [epaxos/scan-map (fn [_ prefix barrier]
                                    (if (and (= "tx-" prefix)
                                             (= epaxos/txn-scan-barrier barrier))
                                      [:ok {}]
                                      [:fail {:prefix prefix :barrier barrier}]))]
      (is (= [:ok (into {} (map (fn [{:keys [group keys]}]
                                  [group (mapv (constantly nil) keys)])
                                epaxos/txn-key-groups))]
             (epaxos/scan-values "http://node"))))))

(deftest mutation-result-classifies-submitted-mutations
  (testing "successful mutation responses complete the operation"
    (is (= {:type :ok :f :write :value 6}
           (select-keys (epaxos/mutation-result {:type :invoke :f :write :value 6}
                                                {:status 204})
                        [:type :f :value]))))
  (testing "server-side mutation responses are indeterminate and preserve the status"
    (is (= {:type :info :f :write :value 3 :error 503}
           (select-keys (epaxos/mutation-result {:type :invoke :f :write :value 3}
                                                {:status 503})
                        [:type :f :value :error]))))
  (testing "client-side mutation responses are definite failures and preserve the status"
    (is (= {:type :fail :f :write :value 4 :error 400}
           (select-keys (epaxos/mutation-result {:type :invoke :f :write :value 4}
                                                {:status 400})
                        [:type :f :value :error])))))

(deftest normalize-register-op-treats-delete-as-register-clear
  (testing "successful deletes become nil writes for the knossos register model"
    (is (= {:type :ok :f :write :value nil}
           (epaxos/normalize-register-op {:type :ok :f :delete}))))
  (testing "reads and writes keep their model-visible operation and value"
    (doseq [op [{:type :invoke :f :read :value nil}
                {:type :ok :f :read :value 7}
                {:type :invoke :f :write :value 8}
                {:type :ok :f :write :value 8}]]
      (is (= op (epaxos/normalize-register-op op))))))

(defn check-register-history [history]
  (checker/check (epaxos/register-linearizable-checker) nil history nil))

(deftest register-linearizable-checker-accepts-delete-followed-by-missing-read
  (testing "a delete clears the register so a later nil read remains linearizable"
    (let [history [{:type :invoke :process 0 :f :write :value 1}
                   {:type :ok :process 0 :f :write :value 1}
                   {:type :invoke :process 0 :f :delete}
                   {:type :ok :process 0 :f :delete}
                   {:type :invoke :process 0 :f :read}
                   {:type :ok :process 0 :f :read :value nil}]
          result (check-register-history history)]
      (is (= true (:valid? result))))))

(deftest register-linearizable-checker-accepts-observed-info-write
  (testing "an indeterminate write may be linearized when a later read observes its value"
    (let [history [{:type :invoke :process 0 :f :write :value 6}
                   {:type :ok :process 0 :f :write :value 6}
                   {:type :invoke :process 1 :f :write :value 3}
                   {:type :info :process 1 :f :write :value 3 :error 503}
                   {:type :invoke :process 2 :f :read}
                   {:type :ok :process 2 :f :read :value 3}]
          result (check-register-history history)]
      (is (= true (:valid? result))))))

(deftest kv-client-represents-missing-read-as-nil-value
  (testing "HTTP 404 from the single-register read path is surfaced as an ok nil read"
    (with-redefs [http/get (fn [_ _] {:status 404 :body "missing"})]
      (is (= {:type :ok :f :read :value nil}
             (select-keys (client/invoke! (epaxos/->KVClient "n1")
                                          {}
                                          {:type :invoke :f :read :value :stale})
                          [:type :f :value]))))))

(defn check-txn-history [history]
  (checker/check (epaxos/txn-atomic-checker) nil history nil))

(deftest txn-atomic-checker-checks-each-group-independently
  (testing "values may differ across groups when each group is internally atomic"
    (let [result (check-txn-history [{:type :ok
                                      :f :txn-read
                                      :value {:tx-a [1 1]
                                              :tx-b [2 2 2]
                                              :tx-c [nil nil]}}])]
      (is (= {:valid? true :checked 1 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))))
  (testing "fully deleted transaction groups are atomic reads"
    (let [result (check-txn-history [{:type :ok
                                      :f :txn-read
                                      :value {:tx-a [nil nil]
                                              :tx-b [nil nil nil]
                                              :tx-c [nil nil]}}])]
      (is (= {:valid? true :checked 1 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))))
  (testing "mixed or partial values inside any one group make the read invalid"
    (let [read-op {:type :ok
                   :f :txn-read
                   :value {:tx-a [1 2]
                           :tx-b [9 9 9]
                           :tx-c [nil 3]}}
          result (check-txn-history [read-op])
          bad-op (first (:bad result))]
      (is (= {:valid? false :checked 1 :bad-count 1}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= {:tx-a [1 2] :tx-c [nil 3]}
             (:bad-groups bad-op)))))
  (testing "mixed delete and write visibility inside a group is invalid"
    (let [read-op {:type :ok
                   :f :txn-read
                   :value {:tx-a [4 4]
                           :tx-b [nil 5 nil]
                           :tx-c [6 6]}}
          result (check-txn-history [read-op])
          bad-op (first (:bad result))]
      (is (= {:valid? false :checked 1 :bad-count 1}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= {:tx-b [nil 5 nil]}
             (:bad-groups bad-op))))))


(defn check-advanced-scan-history [history]
  (checker/check (epaxos/advanced-scan-checker) nil history nil))

(defn scan-row [key value]
  {:key key :value (pr-str value)})

(deftest advanced-scan-sends-scan-query-params
  (testing "forward scans send the prefix, string limit, and barrier without a reverse flag"
    (let [requests (atom [])
          rows [(scan-row "scan-a" :old)]]
      (with-redefs [http/get (fn [url opts]
                               (swap! requests conj [url opts])
                               {:status 200 :body (json/write-str rows)})]
        (is (= [:ok rows]
               (epaxos/advanced-scan "http://node" false)))
        (is (= [["http://node/scan"
                 {:query-params {"prefix" epaxos/scan-prefix
                                 "limit" (str epaxos/scan-limit)
                                 "barrier" epaxos/scan-barrier}
                  :throw-exceptions false}]]
               @requests)))))
  (testing "reverse scans send the same scan shape params plus reverse true"
    (let [requests (atom [])
          rows [(scan-row "scan-c" :old)]]
      (with-redefs [http/get (fn [url opts]
                               (swap! requests conj [url opts])
                               {:status 200 :body (json/write-str rows)})]
        (is (= [:ok rows]
               (epaxos/advanced-scan "http://node" true)))
        (is (= [["http://node/scan"
                 {:query-params {"prefix" epaxos/scan-prefix
                                 "limit" (str epaxos/scan-limit)
                                 "reverse" "true"
                                 "barrier" epaxos/scan-barrier}
                  :throw-exceptions false}]]
               @requests))))))

(deftest advanced-scan-checker-accepts-sorted-scan-shapes
  (testing "ok scans are checked for key shape, not returned values"
    (let [history [{:type :ok
                    :f :scan-forward
                    :value [(scan-row "scan-a" :stale)
                            (scan-row "scan-b" :newer)
                            (scan-row "scan-c" :older)]}
                   {:type :ok
                    :f :scan-reverse
                    :value [(scan-row "scan-d" 4)
                            (scan-row "scan-c" 1)
                            (scan-row "scan-a" 9)]}]
          result (check-advanced-scan-history history)]
      (is (= {:valid? true :checked 2 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [] (vec (:bad result)))))))

(deftest advanced-scan-checker-rejects-bad-scan-shapes
  (testing "scan failures identify whether the limit, prefix, or direction order was broken"
    (let [over-limit {:type :ok
                      :f :scan-forward
                      :value (mapv #(scan-row (str "scan-" %) %)
                                   (range (inc epaxos/scan-limit)))}
          bad-prefix {:type :ok
                      :f :scan-forward
                      :value [(scan-row "scan-a" 1)
                              (scan-row "other-a" 2)]}
          forward-order {:type :ok
                         :f :scan-forward
                         :value [(scan-row "scan-b" 2)
                                 (scan-row "scan-a" 1)]}
          reverse-order {:type :ok
                         :f :scan-reverse
                         :value [(scan-row "scan-a" 1)
                                 (scan-row "scan-b" 2)]}
          result (check-advanced-scan-history [over-limit
                                               bad-prefix
                                               forward-order
                                               reverse-order])
          bad (vec (:bad result))]
      (is (= {:valid? false :checked 4 :bad-count 4}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [{:f :scan-forward :bad-scan :limit}
              {:f :scan-forward :bad-scan :prefix}
              {:f :scan-forward :bad-scan :order}
              {:f :scan-reverse :bad-scan :order}]
             (mapv #(select-keys % [:f :bad-scan]) bad))))))

(deftest advanced-scan-checker-ignores-non-scan-and-failed-scans
  (testing "only ok forward and reverse scan operations are checked"
    (let [history [{:type :ok
                    :f :read
                    :value [(scan-row "other-a" 1)]}
                   {:type :fail
                    :f :scan-forward
                    :value [(scan-row "other-a" 1)]}
                   {:type :info
                    :f :scan-reverse
                    :value [(scan-row "scan-a" 1)
                            (scan-row "scan-b" 2)]}]
          result (check-advanced-scan-history history)]
      (is (= {:valid? true :checked 0 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [] (vec (:bad result)))))))