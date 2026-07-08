(ns moreconsensus.epaxos-test-test
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.client :as client]
            [jepsen.control :as control]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [moreconsensus.epaxos-test :as epaxos]))

(defn fault-env-from [values]
  (fn [suffix]
    (get values suffix)))

(def client-operation-fs
  #{:read :write :delete
    :scan-write :scan-delete :scan-forward :scan-reverse
    :scan-at :scan-bounded-staleness :scan-exact-staleness
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

(deftest remote-config-is-opt-in-and-builds-peer-urls-for-bare-nodes
  (let [nodes ["alpha" "bravo"]
        env {"REMOTE" "yes"
             "FAULTS" "restart"
             "BIN" "/opt/kvnode"
             "HTTP_PORT" "19090"
             "REMOTE_DIR" "/srv/kv"}]
    (testing "remote harness stays disabled unless explicitly requested"
      (with-redefs [epaxos/fault-env (fault-env-from (assoc env "REMOTE" "0"))]
        (is (nil? (epaxos/remote-config {:nodes nodes})))))
    (testing "bare hostnames get stable node ids and peer URLs on the configured HTTP port"
      (with-redefs [epaxos/fault-env (fault-env-from env)]
        (let [cfg (epaxos/remote-config {:nodes nodes})]
          (is (= {:faults "restart"
                  :bin "/opt/kvnode"
                  :remote-dir "/srv/kv"
                  :http-port 19090
                  :nodes nodes
                  :node-ids {"alpha" 1 "bravo" 2}
                  :peers "1=http://alpha:19090,2=http://bravo:19090"}
                 cfg))
          (is (= "1=http://alpha:19090,2=http://bravo:19090"
                 (epaxos/peer-spec cfg)))
          (is (= "alpha:19090"
                 (epaxos/http-node cfg "alpha")))
          (is (= "bravo:7777"
                 (epaxos/http-node cfg "bravo:7777"))))))))

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

(deftest local-destructive-storage-generator-emits-remove-and-restore-in-order
  (let [nodes ["127.0.0.1:9101" "127.0.0.1:9102"]
        ops (take 9 (epaxos/destructive-storage-generator nodes))]
    (is (= [{:type :info :f :remove-storage :value "127.0.0.1:9101"}
            {:type :info :f :restore-storage :value "127.0.0.1:9101"}
            {:type :info :f :remove-storage :value "127.0.0.1:9102"}
            {:type :info :f :restore-storage :value "127.0.0.1:9102"}]
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

(deftest client-workload-generator-seeds-scan-keys-before-stale-scans
  (testing "stale scan queries are generated only after deterministic scan writes and a learning scan"
    (let [expected-stage-shape (vec (concat (map (fn [key]
                                                   {:type :invoke
                                                    :f :scan-write
                                                    :key key
                                                    :has-value? true})
                                                 epaxos/scan-keys)
                                            [{:type :invoke
                                              :f :scan-forward
                                              :value nil}
                                             {:type :invoke
                                              :f :scan-at
                                              :value nil}
                                             {:type :invoke
                                              :f :scan-bounded-staleness
                                              :value nil}
                                             {:type :invoke
                                              :f :scan-exact-staleness
                                              :value nil}]))
          relevant-fs #{:scan-write
                        :scan-forward
                        :scan-at
                        :scan-bounded-staleness
                        :scan-exact-staleness}
          stage (->> (generated-ops-for (epaxos/client-workload-generator)
                                        0
                                        40)
                     (filter #(contains? relevant-fs (:f %)))
                     (take (count expected-stage-shape))
                     vec)
          stage-shape (mapv (fn [op]
                              (if (= :scan-write (:f op))
                                {:type (:type op)
                                 :f (:f op)
                                 :key (get-in op [:value :key])
                                 :has-value? (contains? (:value op) :value)}
                                (select-keys op [:type :f :value])))
                            stage)]
      (is (= expected-stage-shape stage-shape)
          (str "expected the stale scan seed/learn/query stage before the mixed workload; saw "
               stage)))))

(deftest start-node-reports-healthy-launch
  (let [cfg {:base-port 9100
             :data-dir "/var/lib/moreconsensus"
             :peers "127.0.0.1:9101,127.0.0.1:9102"
             :pid-dir "/var/run/moreconsensus"}
        node "127.0.0.1:9101"
        calls (atom [])]
    (with-redefs [epaxos/read-pid (fn [seen-cfg seen-node]
                                    (swap! calls conj [:read seen-cfg seen-node])
                                    nil)
                  epaxos/pid-alive? (fn [seen-pid]
                                      (swap! calls conj [:alive seen-pid])
                                      false)
                  epaxos/launch-node-process! (fn [seen-cfg seen-node]
                                                (swap! calls conj [:launch seen-cfg seen-node])
                                                "4242")
                  epaxos/wait-node-health (fn [seen-cfg seen-node]
                                            (swap! calls conj [:health seen-cfg seen-node])
                                            {:node seen-node
                                             :status 200
                                             :healthy true
                                             :attempt 1})
                  epaxos/write-pid! (fn [seen-cfg seen-node seen-pid]
                                      (swap! calls conj [:write seen-cfg seen-node seen-pid]))]
      (is (= {:node node
              :pid "4242"
              :action :started
              :status 200
              :healthy true
              :attempt 1}
             (epaxos/start-node! cfg node)))
      (is (= [[:read cfg node]
              [:alive nil]
              [:launch cfg node]
              [:health cfg node]
              [:write cfg node "4242"]]
             @calls)))))

(deftest local-restart-nemesis-reports-unhealthy-restart
  (let [cfg {:faults "restart"
             :nodes ["127.0.0.1:9101"]}
        node "127.0.0.1:9101"
        health-calls (atom [])
        launches (atom [])
        writes (atom [])]
    (with-redefs [epaxos/read-pid (fn [_ _] nil)
                  epaxos/pid-alive? (constantly false)
                  epaxos/launch-node-process! (fn [seen-cfg seen-node]
                                                (swap! launches conj [seen-cfg seen-node])
                                                "5151")
                  epaxos/health-attempts 3
                  epaxos/health-pause-ms 0
                  epaxos/node-health (fn [seen-cfg seen-node]
                                       (swap! health-calls conj [seen-cfg seen-node])
                                       {:node seen-node
                                        :status 503
                                        :healthy false})
                  epaxos/write-pid! (fn [seen-cfg seen-node seen-pid]
                                      (swap! writes conj [seen-cfg seen-node seen-pid]))]
      (let [nemesis (epaxos/local-fault-nemesis cfg)
            restart-op (nemesis/invoke! nemesis {}
                                        {:type :invoke
                                         :f :restart-node
                                         :value node})]
        (is (= [[cfg node]]
               @launches))
        (is (= [[cfg node] [cfg node] [cfg node]]
               @health-calls))
        (is (= [[cfg node "5151"]]
               @writes))
        (is (= {:type :info
                :f :restart-node
                :value {:node node
                        :pid "5151"
                        :action :start-failed
                        :status 503
                        :healthy false
                        :attempt 3}}
               (select-keys restart-op [:type :f :value])))))))

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

(deftest remove-local-storage-stops-node-and-moves-data-aside
  (let [root (doto (java.io.File/createTempFile "epaxos-storage" "local")
               (.delete)
               (.mkdirs))
        cfg {:data-dir (.getPath root)
             :base-port 9100}
        node "127.0.0.1:9101"
        data-dir (epaxos/local-data-dir cfg node)
        backup-dir (epaxos/local-storage-backup-dir cfg node)
        calls (atom [])]
    (try
      (.mkdirs data-dir)
      (spit (io/file data-dir "state.edn") "{:committed 7}")
      (with-redefs [epaxos/stop-node! (fn [seen-cfg seen-node]
                                        (swap! calls conj [:stop seen-cfg seen-node])
                                        {:node seen-node :action :stopped})
                    epaxos/start-node! (fn [_ _]
                                         (throw (ex-info "storage removal must keep the node stopped" {})))]
        (is (= {:node node
                :action :storage-removed
                :stop {:node node :action :stopped}}
               (epaxos/remove-local-storage! cfg node)))
        (is (= [[:stop cfg node]]
               @calls))
        (is (false? (.exists data-dir)))
        (is (= "{:committed 7}"
               (slurp (io/file backup-dir "state.edn")))))
      (finally
        (epaxos/local-rm-rf! root)))))

(deftest restore-local-storage-restores-backup-before-restart
  (let [root (doto (java.io.File/createTempFile "epaxos-storage" "restore")
               (.delete)
               (.mkdirs))
        cfg {:data-dir (.getPath root)
             :base-port 9100}
        node "127.0.0.1:9101"
        data-dir (epaxos/local-data-dir cfg node)
        backup-dir (epaxos/local-storage-backup-dir cfg node)
        calls (atom [])]
    (try
      (.mkdirs data-dir)
      (spit (io/file data-dir "stale.edn") "{:stale true}")
      (.mkdirs backup-dir)
      (spit (io/file backup-dir "state.edn") "{:committed 9}")
      (with-redefs [epaxos/stop-node! (fn [seen-cfg seen-node]
                                        (swap! calls conj [:stop seen-cfg seen-node])
                                        {:node seen-node :action :stopped})
                    epaxos/start-node! (fn [seen-cfg seen-node]
                                         (swap! calls conj [:start seen-cfg seen-node])
                                         {:node seen-node :action :started})]
        (is (= {:node node
                :action :storage-restored
                :stop {:node node :action :stopped}
                :start {:node node :action :started}}
               (epaxos/restore-local-storage! cfg node)))
        (is (= [[:stop cfg node]
                [:start cfg node]]
               @calls))
        (is (false? (.exists backup-dir)))
        (is (false? (.exists (io/file data-dir "stale.edn"))))
        (is (= "{:committed 9}"
               (slurp (io/file data-dir "state.edn")))))
      (finally
        (epaxos/local-rm-rf! root)))))

(deftest local-destructive-storage-nemesis-removes-and-restores-selected-storage
  (let [cfg {:faults "destructive-storage"
             :nodes ["127.0.0.1:9101" "127.0.0.1:9102"]}
        calls (atom [])]
    (with-redefs [epaxos/remove-local-storage! (fn [seen-cfg node]
                                                (swap! calls conj [:remove seen-cfg node])
                                                {:node node
                                                 :action :storage-removed
                                                 :stop {:node node :action :stopped}})
                  epaxos/restore-local-storage! (fn [seen-cfg node]
                                                 (swap! calls conj [:restore seen-cfg node])
                                                 {:node node
                                                  :action :storage-restored
                                                  :stop {:node node :action :stopped}
                                                  :start {:node node :action :started}})]
      (let [nemesis (epaxos/local-fault-nemesis cfg)
            remove-op (nemesis/invoke! nemesis {}
                                       {:type :invoke
                                        :f :remove-storage
                                        :value "127.0.0.1:9101"})
            restore-op (nemesis/invoke! nemesis {}
                                        {:type :invoke
                                         :f :restore-storage
                                         :value "127.0.0.1:9102"})]
        (is (= [[:remove cfg "127.0.0.1:9101"]
                [:restore cfg "127.0.0.1:9102"]]
               @calls))
        (is (= {:type :info
                :f :remove-storage
                :value {:node "127.0.0.1:9101"
                        :action :storage-removed
                        :stop {:node "127.0.0.1:9101" :action :stopped}}}
               (select-keys remove-op [:type :f :value])))
        (is (= {:type :info
                :f :restore-storage
                :value {:node "127.0.0.1:9102"
                        :action :storage-restored
                        :stop {:node "127.0.0.1:9102" :action :stopped}
                        :start {:node "127.0.0.1:9102" :action :started}}}
               (select-keys restore-op [:type :f :value])))))))

(deftest remove-remote-storage-stops-node-and-moves-data-aside
  (let [cfg {:remote-dir "/srv/kv"
             :node-ids {"alpha" 1}}
        data-dir "/srv/kv/node-1"
        backup-dir "/srv/kv/node-1.removed"
        calls (atom [])]
    (with-redefs [cu/exists? (fn [path]
                               (swap! calls conj [:exists path])
                               (= data-dir path))
                  control/exec (fn [& args]
                                 (swap! calls conj (into [:exec] args))
                                 {:exit 0})
                  epaxos/stop-remote-node! (fn [seen-cfg node]
                                             (swap! calls conj [:stop seen-cfg node])
                                             {:node node :action :stopped})
                  epaxos/start-remote-node! (fn [_ _]
                                              (throw (ex-info "storage removal must keep the remote node stopped" {})))]
      (is (= {:node "alpha"
              :action :storage-removed
              :stop {:node "alpha" :action :stopped}}
             (epaxos/remove-remote-storage! cfg "alpha")))
      (is (= [[:stop cfg "alpha"]
              [:exists data-dir]
              [:exec :rm :-rf backup-dir]
              [:exec :mv data-dir backup-dir]]
             @calls)))))

(deftest restore-remote-storage-restores-backup-before-restart
  (let [cfg {:remote-dir "/srv/kv"
             :node-ids {"alpha" 1}}
        data-dir "/srv/kv/node-1"
        backup-dir "/srv/kv/node-1.removed"
        calls (atom [])]
    (with-redefs [cu/exists? (fn [path]
                               (swap! calls conj [:exists path])
                               (= backup-dir path))
                  control/exec (fn [& args]
                                 (swap! calls conj (into [:exec] args))
                                 {:exit 0})
                  epaxos/stop-remote-node! (fn [seen-cfg node]
                                             (swap! calls conj [:stop seen-cfg node])
                                             {:node node :action :stopped})
                  epaxos/start-remote-node! (fn [seen-cfg node]
                                              (swap! calls conj [:start seen-cfg node])
                                              {:node node :action :started})]
      (is (= {:node "alpha"
              :action :storage-restored
              :stop {:node "alpha" :action :stopped}
              :start {:node "alpha" :action :started}}
             (epaxos/restore-remote-storage! cfg "alpha")))
      (is (= [[:stop cfg "alpha"]
              [:exec :mv backup-dir data-dir]
              [:start cfg "alpha"]]
             (filterv (fn [call]
                        (or (#{:stop :start} (first call))
                            (= [:exec :mv backup-dir data-dir] call)))
                      @calls)))
      (is (some #{[:exec :rm :-rf data-dir]} @calls)))))

(deftest remote-restore-storage-leaves-live-data-when-backup-is-absent
  (let [cfg {:remote-dir "/srv/kv"
             :node-ids {"alpha" 1}}
        calls (atom [])]
    (with-redefs [cu/exists? (fn [path]
                               (swap! calls conj [:exists path])
                               false)
                  control/exec (fn [& args]
                                 (swap! calls conj (into [:exec] args))
                                 (throw (ex-info "must not remove live data without a backup" {})))
                  epaxos/stop-remote-node! (fn [_ _]
                                             (throw (ex-info "must not stop a node without a backup" {})))
                  epaxos/start-remote-node! (fn [_ _]
                                              (throw (ex-info "must not restart a node without a backup" {})))]
      (is (= {:node "alpha" :action :storage-unchanged}
             (epaxos/restore-remote-storage! cfg "alpha")))
      (is (= [[:exists "/srv/kv/node-1.removed"]]
             @calls)))))

(deftest remote-start-node-constructs-command-from-node-id-port-and-peer-spec
  (let [cfg {:remote-dir "/srv/kv"
             :http-port 19090
             :node-ids {"alpha" 1 "bravo" 2}
             :peers "1=http://alpha:19090,2=http://bravo:19090"}
        calls (atom [])]
    (with-redefs [control/exec (fn [& args]
                                 (swap! calls conj (into [:exec] args))
                                 {:exit 0})
                  cu/start-daemon! (fn [opts cmd & args]
                                     (swap! calls conj [:start-daemon opts cmd (vec args)])
                                     :started)
                  epaxos/wait-node-health (fn [_ node]
                                            (swap! calls conj [:health node])
                                            {:status 200 :healthy true})]
      (is (= {:node "bravo"
              :action :started
              :status 200
              :healthy true}
             (epaxos/start-remote-node! cfg "bravo")))
      (is (= [[:exec :mkdir :-p "/srv/kv/node-2"]
              [:start-daemon {:logfile "/srv/kv/node-2.log"
                              :pidfile "/srv/kv/node-2.pid"
                              :chdir "/srv/kv"}
               "/srv/kv/kvnode"
               ["-id" "2"
                "-listen" ":19090"
                "-data" "/srv/kv/node-2"
                "-peers" "1=http://alpha:19090,2=http://bravo:19090"]]
              [:health "bravo"]]
             @calls)))))

(deftest kvnode-db-setup-uploads-cleans-and-starts-selected-remote-node
  (let [cfg {:bin "/opt/kvnode"
             :remote-dir "/srv/kv"
             :http-port 19090
             :node-ids {"alpha" 1}
             :peers "1=http://alpha:19090"}
        calls (atom [])]
    (with-redefs [control/exec (fn [& args]
                                 (swap! calls conj (into [:exec] args))
                                 {:exit 0})
                  control/upload (fn [source dest]
                                   (swap! calls conj [:upload source dest]))
                  cu/stop-daemon! (fn [pid-file]
                                    (swap! calls conj [:stop-daemon pid-file])
                                    :stopped)
                  cu/start-daemon! (fn [opts cmd & args]
                                     (swap! calls conj [:start-daemon opts cmd (vec args)])
                                     :started)
                  epaxos/wait-node-health (fn [_ node]
                                            (swap! calls conj [:health node])
                                            {:status 200 :healthy true})]
      (is (= {:node "alpha"
              :action :started
              :status 200
              :healthy true}
             (db/setup! (epaxos/kvnode-db cfg) {:http-port 19090} "alpha")))
      (is (= [[:exec :mkdir :-p "/srv/kv"]
              [:upload "/opt/kvnode" "/srv/kv/kvnode"]
              [:exec :chmod :+x "/srv/kv/kvnode"]
              [:stop-daemon "/srv/kv/node-1.pid"]
              [:exec :rm :-rf "/srv/kv/node-1" "/srv/kv/node-1.removed"]
              [:exec :mkdir :-p "/srv/kv/node-1"]
              [:start-daemon {:logfile "/srv/kv/node-1.log"
                              :pidfile "/srv/kv/node-1.pid"
                              :chdir "/srv/kv"}
               "/srv/kv/kvnode"
               ["-id" "1"
                "-listen" ":19090"
                "-data" "/srv/kv/node-1"
                "-peers" "1=http://alpha:19090"]]
              [:health "alpha"]]
             @calls)))))

(deftest remote-restart-nemesis-routes-selected-node-through-control
  (let [cfg {:faults "restart"
             :nodes ["alpha" "bravo"]}
        calls (atom [])]
    (with-redefs [control/on-nodes (fn [test nodes f]
                                     (swap! calls conj [:on-nodes (:name test) nodes])
                                     (into {} (map (fn [node] [node (f test node)]) nodes)))
                  epaxos/stop-remote-node! (fn [seen-cfg node]
                                             (swap! calls conj [:stop seen-cfg node])
                                             {:node node :action :stopped})
                  epaxos/start-remote-node! (fn [seen-cfg node]
                                              (swap! calls conj [:start seen-cfg node])
                                              {:node node :action :started})]
      (let [nemesis (epaxos/remote-restart-nemesis cfg)
            kill-op (nemesis/invoke! nemesis {:name "remote-test"}
                                     {:type :invoke
                                      :f :kill-node
                                      :value "bravo"})
            restart-op (nemesis/invoke! nemesis {:name "remote-test"}
                                        {:type :invoke
                                         :f :restart-node
                                         :value "alpha"})]
        (is (= [[:on-nodes "remote-test" ["bravo"]]
                [:stop cfg "bravo"]
                [:on-nodes "remote-test" ["alpha"]]
                [:start cfg "alpha"]]
               @calls))
        (is (= {:type :info
                :f :kill-node
                :value {:node "bravo" :action :stopped}}
               (select-keys kill-op [:type :f :value])))
        (is (= {:type :info
                :f :restart-node
                :value {:node "alpha" :action :started}}
               (select-keys restart-op [:type :f :value])))))))

(deftest remote-destructive-storage-nemesis-routes-selected-node-through-control
  (let [cfg {:faults "destructive-storage"
             :nodes ["alpha" "bravo"]}
        calls (atom [])]
    (with-redefs [control/on-nodes (fn [test nodes f]
                                     (swap! calls conj [:on-nodes (:name test) nodes])
                                     (into {} (map (fn [node] [node (f test node)]) nodes)))
                  epaxos/remove-remote-storage! (fn [seen-cfg node]
                                                 (swap! calls conj [:remove seen-cfg node])
                                                 {:node node
                                                  :action :storage-removed
                                                  :stop {:node node :action :stopped}})
                  epaxos/restore-remote-storage! (fn [seen-cfg node]
                                                  (swap! calls conj [:restore seen-cfg node])
                                                  {:node node
                                                   :action :storage-restored
                                                   :stop {:node node :action :stopped}
                                                   :start {:node node :action :started}})]
      (let [nemesis (epaxos/remote-destructive-storage-nemesis cfg)
            remove-op (nemesis/invoke! nemesis {:name "remote-test"}
                                       {:type :invoke
                                        :f :remove-storage
                                        :value "bravo"})
            restore-op (nemesis/invoke! nemesis {:name "remote-test"}
                                        {:type :invoke
                                         :f :restore-storage
                                         :value "alpha"})]
        (is (= [[:on-nodes "remote-test" ["bravo"]]
                [:remove cfg "bravo"]
                [:on-nodes "remote-test" ["alpha"]]
                [:restore cfg "alpha"]]
               @calls))
        (is (= {:type :info
                :f :remove-storage
                :value {:node "bravo"
                        :action :storage-removed
                        :stop {:node "bravo" :action :stopped}}}
               (select-keys remove-op [:type :f :value])))
        (is (= {:type :info
                :f :restore-storage
                :value {:node "alpha"
                        :action :storage-restored
                        :stop {:node "alpha" :action :stopped}
                        :start {:node "alpha" :action :started}}}
               (select-keys restore-op [:type :f :value])))))))

(deftest remote-storage-http-faults-use-configured-port-for-bare-hostnames
  (let [cfg {:faults "storage"
             :http-port 19090
             :nodes ["alpha" "bravo"]}
        requests (atom [])]
    (with-redefs [http/post (fn [url opts]
                              (swap! requests conj [url opts])
                              {:status 204})]
      (let [nemesis (epaxos/remote-fault-nemesis cfg)
            op (nemesis/invoke! nemesis {}
                                {:type :invoke
                                 :f :fail-storage
                                 :value "alpha"})]
        (is (= {:type :info
                :f :fail-storage
                :value {:node "alpha:19090" :fail true :status 204}}
               (select-keys op [:type :f :value])))
        (is (= [["http://alpha:19090/faults/storage"
                 {:body (json/write-str {"fail" true})
                  :content-type :json
                  :throw-exceptions false}]]
               @requests))))))

(deftest moreconsensus-test-prefers-remote-faults-when-local-env-also-matches
  (let [nodes ["alpha" "bravo"]
        env {"REMOTE" "true"
             "FAULTS" "restart"
             "BIN" "/opt/kvnode"
             "DATA_DIR" "/tmp/data"
             "PEERS" "127.0.0.1:9001,127.0.0.1:9002"
             "PID_DIR" "/tmp/pids"
             "BASE_PORT" "9000"
             "HTTP_PORT" "19090"
             "REMOTE_DIR" "/srv/kv"}
        calls (atom [])]
    (with-redefs [epaxos/fault-env (fault-env-from env)
                  control/on-nodes (fn [test selected-nodes f]
                                     (swap! calls conj [:on-nodes selected-nodes (:http-port test)])
                                     (into {} (map (fn [node] [node (f test node)]) selected-nodes)))
                  epaxos/stop-remote-node! (fn [cfg node]
                                             (swap! calls conj [:remote-stop (:peers cfg) node])
                                             {:node node :action :stopped})
                  epaxos/stop-node! (fn [_ _]
                                      (throw (ex-info "local restart nemesis should not be selected" {})))]
      (let [test (epaxos/moreconsensus-test {:nodes nodes})
            op (nemesis/invoke! (:nemesis test) test
                                {:type :invoke
                                 :f :kill-node
                                 :value "bravo"})]
        (is (= 19090 (:http-port test)))
        (is (= [[:on-nodes ["bravo"] 19090]
                [:remote-stop "1=http://alpha:19090,2=http://bravo:19090" "bravo"]]
               @calls))
        (is (= {:type :info
                :f :kill-node
                :value {:node "bravo" :action :stopped}}
               (select-keys op [:type :f :value])))))))

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

(defn index-history [history]
  (mapv (fn [index op] (cond-> op (nil? (:index op)) (assoc :index index))) (range) history))

(defn check-txn-history [history]
  (checker/check (epaxos/txn-atomic-checker)
                 {:name "txn-atomic-checker-test" :start-time 0}
                 (index-history history)
                 nil))

(defn txn-read-state [values-by-group]
  (into {} (map (fn [{:keys [group keys]}]
                  [group (vec (repeat (count keys) (get values-by-group group)))])
                epaxos/txn-key-groups)))

(deftest txn-atomic-checker-checks-each-group-independently
  (testing "values may differ across groups when each group is internally atomic"
    (let [result (check-txn-history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-a :value 1}}
                                     {:type :ok :process 0 :f :txn-write :value {:group :tx-a :value 1}}
                                     {:type :invoke :process 1 :f :txn-write :value {:group :tx-b :value 2}}
                                     {:type :ok :process 1 :f :txn-write :value {:group :tx-b :value 2}}
                                     {:type :invoke :process 2 :f :txn-read :value nil}
                                     {:type :ok :process 2 :f :txn-read :value (txn-read-state {:tx-a 1 :tx-b 2})}])]
      (is (= {:valid? true :checked 1 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))))
  (testing "fully deleted transaction groups are atomic reads"
    (let [result (check-txn-history [{:type :invoke :process 0 :f :txn-read :value nil}
                                     {:type :ok
                                      :process 0
                                      :f :txn-read
                                      :value {:tx-a [nil nil]
                                              :tx-b [nil nil nil]
                                              :tx-c [nil nil]}}])]
      (is (= {:valid? true :checked 1 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))))
  (testing "mixed or partial values inside any one group make the read invalid"
    (let [read-op {:type :ok
                   :process 0
                   :f :txn-read
                   :value {:tx-a [1 2]
                           :tx-b [9 9 9]
                           :tx-c [nil 3]}}
          result (check-txn-history [{:type :invoke :process 0 :f :txn-read :value nil}
                                     read-op])
          bad-op (first (:bad result))]
      (is (= {:valid? false :checked 1 :bad-count 1}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= {:tx-a [1 2] :tx-c [nil 3]}
             (:bad-groups bad-op)))))
  (testing "mixed delete and write visibility inside a group is invalid"
    (let [read-op {:type :ok
                   :process 0
                   :f :txn-read
                   :value {:tx-a [4 4]
                           :tx-b [nil 5 nil]
                           :tx-c [6 6]}}
          result (check-txn-history [{:type :invoke :process 0 :f :txn-read :value nil}
                                     read-op])
          bad-op (first (:bad result))]
      (is (= {:valid? false :checked 1 :bad-count 1}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= {:tx-b [nil 5 nil]}
             (:bad-groups bad-op))))))

(deftest txn-atomic-checker-enforces-completed-write-visibility
  (testing "a later read observing the completed write remains valid"
    (let [history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-a :value :committed}}
                   {:type :ok :process 0 :f :txn-write :value {:group :tx-a :value :committed}}
                   {:type :invoke :process 1 :f :txn-read :value nil}
                   {:type :ok :process 1 :f :txn-read :value (txn-read-state {:tx-a :committed})}]
          result (check-txn-history history)]
      (is (= true (:valid? result)))))
  (testing "a later read of an older value after the completed write is invalid"
    (let [history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-a :value :old}}
                   {:type :ok :process 0 :f :txn-write :value {:group :tx-a :value :old}}
                   {:type :invoke :process 1 :f :txn-write :value {:group :tx-a :value :committed}}
                   {:type :ok :process 1 :f :txn-write :value {:group :tx-a :value :committed}}
                   {:type :invoke :process 2 :f :txn-read :value nil}
                   {:type :ok :process 2 :f :txn-read :value (txn-read-state {:tx-a :old})}]
          result (check-txn-history history)]
      (is (= false (:valid? result))))))

(deftest txn-atomic-checker-enforces-completed-delete-visibility
  (testing "a later read observing the completed delete remains valid"
    (let [history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-b :value :old}}
                   {:type :ok :process 0 :f :txn-write :value {:group :tx-b :value :old}}
                   {:type :invoke :process 1 :f :txn-delete :value {:group :tx-b}}
                   {:type :ok :process 1 :f :txn-delete :value {:group :tx-b}}
                   {:type :invoke :process 2 :f :txn-read :value nil}
                   {:type :ok :process 2 :f :txn-read :value (txn-read-state {})}]
          result (check-txn-history history)]
      (is (= true (:valid? result)))))
  (testing "a later read of the deleted value is invalid"
    (let [history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-b :value :old}}
                   {:type :ok :process 0 :f :txn-write :value {:group :tx-b :value :old}}
                   {:type :invoke :process 1 :f :txn-delete :value {:group :tx-b}}
                   {:type :ok :process 1 :f :txn-delete :value {:group :tx-b}}
                   {:type :invoke :process 2 :f :txn-read :value nil}
                   {:type :ok :process 2 :f :txn-read :value (txn-read-state {:tx-b :old})}]
          result (check-txn-history history)]
      (is (= false (:valid? result))))))

(deftest txn-atomic-checker-rejects-never-written-equal-transaction-value
  (testing "equal values across every key in a group still require a prior write"
    (let [history [{:type :invoke :process 0 :f :txn-read :value nil}
                   {:type :ok :process 0 :f :txn-read :value (txn-read-state {:tx-c :phantom})}]
          result (check-txn-history history)]
      (is (= false (:valid? result))))))

(deftest txn-atomic-checker-accepts-observed-indeterminate-write
  (testing "an info write may be linearized when a later transaction read observes it"
    (let [history [{:type :invoke :process 0 :f :txn-write :value {:group :tx-a :value :maybe}}
                   {:type :info :process 0 :f :txn-write :value {:group :tx-a :value :maybe} :error 503}
                   {:type :invoke :process 1 :f :txn-read :value nil}
                   {:type :ok :process 1 :f :txn-read :value (txn-read-state {:tx-a :maybe})}]
          result (check-txn-history history)]
      (is (= true (:valid? result))))))

(deftest txn-atomic-checker-rejects-missing-and-wrong-sized-groups
  (testing "every transaction read must contain exactly the configured groups with one value per key"
    (let [missing-group {:type :ok
                         :process 0
                         :f :txn-read
                         :value {:tx-a [1 1]
                                 :tx-c [3 3]}}
          wrong-lengths {:type :ok
                         :process 1
                         :f :txn-read
                         :value {:tx-a [7]
                                 :tx-b [8 8]
                                 :tx-c [9 9 9]}}
          result (check-txn-history [{:type :invoke :process 0 :f :txn-read :value nil}
                                     missing-group
                                     {:type :invoke :process 1 :f :txn-read :value nil}
                                     wrong-lengths])]
      (is (= {:valid? false :checked 2 :bad-count 2}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [(:value missing-group) (:value wrong-lengths)]
             (mapv :value (:bad result)))))))


(defn check-advanced-scan-history [history]
  (checker/check (epaxos/advanced-scan-checker) nil history nil))

(defn scan-row [key value]
  {:key key :value (pr-str value) :time 0})

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
  (testing "ok scans are checked for key order and EDN row values"
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

(deftest advanced-scan-checker-rejects-malformed-scan-rows
  (testing "scan rows must be maps with string keys and EDN-encoded string values"
    (let [non-map-row {:type :ok
                       :f :scan-forward
                       :value ["not-a-row"]}
          missing-value {:type :ok
                         :f :scan-forward
                         :value [{:key "scan-a"}]}
          non-string-key {:type :ok
                          :f :scan-forward
                          :value [{:key :scan-a :value (pr-str 1) :time 0}]}
          non-string-value {:type :ok
                            :f :scan-forward
                            :value [{:key "scan-a" :value 1 :time 0}]}
          non-edn-value {:type :ok
                         :f :scan-forward
                         :value [{:key "scan-a" :value "{:unterminated" :time 0}]}
          result (check-advanced-scan-history [non-map-row
                                               missing-value
                                               non-string-key
                                               non-string-value
                                               non-edn-value])]
      (is (= {:valid? false :checked 5 :bad-count 5}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [(:value non-map-row)
              (:value missing-value)
              (:value non-string-key)
              (:value non-string-value)
              (:value non-edn-value)]
             (mapv :value (:bad result)))))))

(deftest advanced-scan-checker-rejects-duplicate-scan-keys
  (testing "scan results must not repeat a key even when they satisfy prefix, order, and limit"
    (let [forward-duplicate {:type :ok
                             :f :scan-forward
                             :value [(scan-row "scan-a" 1)
                                     (scan-row "scan-a" 2)
                                     (scan-row "scan-b" 3)]}
          reverse-duplicate {:type :ok
                             :f :scan-reverse
                             :value [(scan-row "scan-c" 3)
                                     (scan-row "scan-c" 2)
                                     (scan-row "scan-b" 1)]}
          result (check-advanced-scan-history [forward-duplicate reverse-duplicate])]
      (is (= {:valid? false :checked 2 :bad-count 2}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [:scan-forward :scan-reverse]
             (mapv :f (:bad result)))))))

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

(defn timed-scan-row [key value time]
  (assoc (scan-row key value) :time time))

(defn required-epaxos-var [sym]
  (or (ns-resolve 'moreconsensus.epaxos-test sym)
      (throw (ex-info (str "missing production var " sym) {:var sym}))))

(defn check-stale-scan-history [history]
  (checker/check ((required-epaxos-var 'stale-scan-checker))
                 {:name "stale-scan-checker-test" :start-time 0}
                 (index-history history)
                 nil))

(deftest stale-scan-client-sends-query-params-and-preserves-rows
  (testing "each stale scan operation calls /scan with scan prefix and records the query beside response rows"
    (doseq [{:keys [f query expected-params rows]}
            [{:f :scan-at
              :query {:at 120}
              :expected-params {"prefix" epaxos/scan-prefix
                                "at" "120"}
              :rows [(timed-scan-row "scan-a" :old 120)]}
             {:f :scan-bounded-staleness
              :query {:reference-time 200 :max-staleness 25}
              :expected-params {"prefix" epaxos/scan-prefix
                                "reference-time" "200"
                                "max-staleness" "25"}
              :rows [(timed-scan-row "scan-a" :old 175)
                     (timed-scan-row "scan-b" :newer 200)]}
             {:f :scan-exact-staleness
              :query {:reference-time 200 :exact-staleness 25}
              :expected-params {"prefix" epaxos/scan-prefix
                                "reference-time" "200"
                                "exact-staleness" "25"}
              :rows [(timed-scan-row "scan-a" :old 175)]}]]
      (let [requests (atom [])
            expected-query (assoc query :prefix epaxos/scan-prefix)]
        (with-redefs [http/get (fn [url opts]
                                 (swap! requests conj [url opts])
                                 {:status 200 :body (json/write-str rows)})]
          (is (= {:type :ok
                  :f f
                  :value {:query expected-query
                          :rows rows}}
                 (select-keys (client/invoke! (epaxos/->KVClient "http://node")
                                             {}
                                             {:type :invoke :f f :value query})
                              [:type :f :value]))
              (str f " should preserve stale scan query metadata and rows"))
          (is (= [["http://node/scan"
                   {:query-params expected-params
                    :throw-exceptions false}]]
                 @requests)
              (str f " should issue the expected /scan query")))))))

(deftest stale-scan-checker-accepts-valid-results-and-empty-misses
  (testing "row times inside each stale scan window are valid, and empty underflow/no-version results are valid"
    (let [history [{:type :ok
                    :f :scan-at
                    :value {:query {:prefix epaxos/scan-prefix :at 120}
                            :rows [(timed-scan-row "scan-a" :old 119)
                                   (timed-scan-row "scan-b" :at 120)]}}
                   {:type :ok
                    :f :scan-bounded-staleness
                    :value {:query {:prefix epaxos/scan-prefix
                                    :reference-time 200
                                    :max-staleness 25}
                            :rows [(timed-scan-row "scan-a" :oldest 175)
                                   (timed-scan-row "scan-b" :newest 200)]}}
                   {:type :ok
                    :f :scan-exact-staleness
                    :value {:query {:prefix epaxos/scan-prefix
                                    :reference-time 200
                                    :exact-staleness 25}
                            :rows [(timed-scan-row "scan-a" :exact 175)]}}
                   {:type :ok
                    :f :scan-exact-staleness
                    :value {:query {:prefix epaxos/scan-prefix
                                    :reference-time 10
                                    :exact-staleness 25}
                            :rows []}}
                   {:type :ok
                    :f :scan-bounded-staleness
                    :value {:query {:prefix epaxos/scan-prefix
                                    :reference-time 300
                                    :max-staleness 10}
                            :rows []}}]
          result (check-stale-scan-history history)]
      (is (= {:valid? true :checked 5 :bad-count 0}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [] (vec (:bad result)))))))

(deftest stale-scan-checker-rejects-malformed-duplicate-and-wrong-prefix-rows
  (testing "stale scan rows must be well-formed, unique, and under the requested scan prefix"
    (let [malformed-row {:type :ok
                         :f :scan-at
                         :value {:query {:prefix epaxos/scan-prefix :at 120}
                                 :rows ["not-a-row"]}}
          missing-time {:type :ok
                        :f :scan-at
                        :value {:query {:prefix epaxos/scan-prefix :at 120}
                                :rows [{:key "scan-a" :value (pr-str :old)}]}}
          duplicate-key {:type :ok
                         :f :scan-bounded-staleness
                         :value {:query {:prefix epaxos/scan-prefix
                                         :reference-time 200
                                         :max-staleness 25}
                                 :rows [(timed-scan-row "scan-a" :older 175)
                                        (timed-scan-row "scan-a" :newer 180)]}}
          outside-prefix {:type :ok
                          :f :scan-exact-staleness
                          :value {:query {:prefix epaxos/scan-prefix
                                          :reference-time 200
                                          :exact-staleness 25}
                                  :rows [(timed-scan-row "other-a" :value 175)]}}
          result (check-stale-scan-history [malformed-row
                                            missing-time
                                            duplicate-key
                                            outside-prefix])]
      (is (= {:valid? false :checked 4 :bad-count 4}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [{:f :scan-at :bad-stale-scan :row}
              {:f :scan-at :bad-stale-scan :time}
              {:f :scan-bounded-staleness :bad-stale-scan :duplicate-key}
              {:f :scan-exact-staleness :bad-stale-scan :prefix}]
             (mapv #(select-keys % [:f :bad-stale-scan]) (:bad result)))))))

(deftest stale-scan-checker-rejects-row-times-outside-query-bounds
  (testing "at and bounded scans reject rows newer/older than their query window, and exact scans require one exact timestamp"
    (let [newer-than-at {:type :ok
                         :f :scan-at
                         :value {:query {:prefix epaxos/scan-prefix :at 120}
                                 :rows [(timed-scan-row "scan-a" :newer 121)]}}
          older-than-bounded-window {:type :ok
                                     :f :scan-bounded-staleness
                                     :value {:query {:prefix epaxos/scan-prefix
                                                     :reference-time 200
                                                     :max-staleness 25}
                                             :rows [(timed-scan-row "scan-a" :too-old 174)]}}
          newer-than-reference {:type :ok
                                :f :scan-bounded-staleness
                                :value {:query {:prefix epaxos/scan-prefix
                                                :reference-time 200
                                                :max-staleness 25}
                                        :rows [(timed-scan-row "scan-a" :too-new 201)]}}
          not-exact-staleness {:type :ok
                                :f :scan-exact-staleness
                                :value {:query {:prefix epaxos/scan-prefix
                                                :reference-time 200
                                                :exact-staleness 25}
                                        :rows [(timed-scan-row "scan-a" :off-by-one 176)]}}
          result (check-stale-scan-history [newer-than-at
                                            older-than-bounded-window
                                            newer-than-reference
                                            not-exact-staleness])]
      (is (= {:valid? false :checked 4 :bad-count 4}
             (select-keys result [:valid? :checked :bad-count])))
      (is (= [{:f :scan-at :bad-stale-scan :time}
              {:f :scan-bounded-staleness :bad-stale-scan :time}
              {:f :scan-bounded-staleness :bad-stale-scan :time}
              {:f :scan-exact-staleness :bad-stale-scan :exact-staleness}]
             (mapv #(select-keys % [:f :bad-stale-scan]) (:bad result)))))))

(deftest workload-checker-includes-stale-scan-shape-validation
  (testing "the composed workload checker rejects malformed successful stale scans"
    (let [noop-checker (reify checker/Checker
                         (check [_ _ _ _] {:valid? true}))
          history (index-history [{:type :ok
                                   :process 0
                                   :f :scan-at
                                   :value {:query {:prefix epaxos/scan-prefix :at 100}
                                           :rows [(timed-scan-row "other-a" :leak 100)]}}])]
      (with-redefs [timeline/html (fn [] noop-checker)]
        (let [result (checker/check (:checker (epaxos/workload))
                                    {:name "stale-scan-workload-test" :start-time 0}
                                    history
                                    nil)]
          (is (= false (:valid? result)))
          (is (= {:valid? false :checked 1 :bad-count 1}
                 (select-keys (:stale-scan-shape result) [:valid? :checked :bad-count])))
          (is (= [{:f :scan-at :bad-stale-scan :prefix}]
                 (mapv #(select-keys % [:f :bad-stale-scan])
                       (get-in result [:stale-scan-shape :bad])))))))))