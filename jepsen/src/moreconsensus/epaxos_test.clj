(ns moreconsensus.epaxos-test
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.control :as control]
            [jepsen.control.util :as cu]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model])
  (:import [java.lang ProcessBuilder$Redirect]))

(defn endpoint [test node]
  (let [node (str/replace (str node) #"/$" "")]
    (cond
      (str/starts-with? node "http://") node
      (str/starts-with? node "https://") node
      (str/includes? node ":") (str "http://" node)
      :else (str "http://" node ":" (or (:http-port test) 8080)))))

(def register-ops #{:read :write :delete})

(def txn-key-groups
  [{:group :tx-a :keys ["tx-a0" "tx-a1"]}
   {:group :tx-b :keys ["tx-b0" "tx-b1" "tx-b2"]}
   {:group :tx-c :keys ["tx-c0" "tx-c1"]}])

(def txn-group-ids (mapv :group txn-key-groups))
(def txn-keys (vec (mapcat :keys txn-key-groups)))
(def txn-keys-by-group (into {} (map (juxt :group :keys)) txn-key-groups))
(def txn-scan-barrier (str/join "," txn-keys))
(def scan-prefix "scan-")
(def scan-keys ["scan-a" "scan-b" "scan-c" "scan-d"])
(def scan-limit 3)
(def scan-barrier (str/join "," scan-keys))

(def fault-env-prefix "MORECONSENSUS_KVNODE_")

(defn fault-env [suffix]
  (System/getenv (str fault-env-prefix suffix)))

(def destructive-storage-fault "destructive-storage")
(def supported-faults #{"restart" "transport" "storage" destructive-storage-fault})
(def enabled-env-values #{"1" "true" "yes"})

(defn env-enabled? [suffix]
  (contains? enabled-env-values (str/lower-case (or (fault-env suffix) ""))))

(defn parse-env-long [suffix default]
  (Long/parseLong (or (fault-env suffix) (str default))))

(defn node-port [node]
  (Long/parseLong (last (str/split (str node) #":"))))

(defn node-http-port [cfg node]
  (if (str/includes? (str node) ":")
    (node-port node)
    (:http-port cfg)))

(defn local-node-id [cfg node]
  (- (node-port node) (:base-port cfg)))

(defn indexed-node-ids [nodes]
  (into {} (map-indexed (fn [idx node] [node (inc idx)]) nodes)))

(defn node-id [cfg node]
  (or (get (:node-ids cfg) node)
      (local-node-id cfg node)))

(defn peer-spec [cfg]
  (str/join "," (map (fn [node]
                       (str (node-id cfg node) "=" (endpoint cfg node)))
                     (:nodes cfg))))

(defn http-node [cfg node]
  (if (or (nil? (:http-port cfg)) (str/includes? (str node) ":"))
    node
    (str node ":" (:http-port cfg))))

(defn local-fault-config [opts]
  (let [faults (fault-env "FAULTS")
        base-port (parse-env-long "BASE_PORT" 0)
        nodes (:nodes opts)]
    (cond
      (#{"restart" destructive-storage-fault} faults)
      (let [cfg {:faults faults
                 :bin (fault-env "BIN")
                 :data-dir (fault-env "DATA_DIR")
                 :peers (fault-env "PEERS")
                 :pid-dir (fault-env "PID_DIR")
                 :base-port base-port
                 :nodes nodes}]
        (when (and (every? seq ((juxt :bin :data-dir :peers :pid-dir :nodes) cfg))
                   (pos? (:base-port cfg)))
          cfg))

      (#{"transport" "storage"} faults)
      (let [cfg {:faults faults
                 :base-port base-port
                 :nodes nodes}]
        (when (and (seq nodes) (pos? base-port))
          cfg))

      :else nil)))

(defn pid-file [cfg node]
  (io/file (:pid-dir cfg) (str "node-" (node-id cfg node) ".pid")))

(defn read-pid [cfg node]
  (let [file (pid-file cfg node)]
    (when (.exists file)
      (str/trim (slurp file)))))

(defn pid-alive? [pid]
  (and (seq pid) (zero? (:exit (sh "kill" "-0" pid)))))

(defn remove-pid! [cfg node]
  (io/delete-file (pid-file cfg node) true))

(defn write-pid! [cfg node pid]
  (let [file (pid-file cfg node)]
    (.mkdirs (.getParentFile file))
    (spit file (str pid))))

(defn stop-node! [cfg node]
  (let [pid (read-pid cfg node)]
    (if (pid-alive? pid)
      (do
        (sh "kill" "-TERM" pid)
        (dotimes [_ 20]
          (when (pid-alive? pid)
            (Thread/sleep 50)))
        (when (pid-alive? pid)
          (sh "kill" "-KILL" pid))
        (remove-pid! cfg node)
        {:node node :pid pid :action :killed})
      {:node node :action :already-down})))

(def health-attempts 20)
(def health-pause-ms 50)

(defn node-health [cfg node]
  (try
    (let [resp (http/get (str (endpoint cfg node) "/health")
                         {:throw-exceptions false})]
      {:node node :status (:status resp) :healthy (= 200 (:status resp))})
    (catch Exception e
      {:node node :healthy false :error (.getMessage e)})))

(defn wait-node-health [cfg node]
  (loop [attempt 1
         last-health nil]
    (let [health (assoc (node-health cfg node) :attempt attempt)]
      (if (:healthy health)
        health
        (if (< attempt health-attempts)
          (do
            (Thread/sleep health-pause-ms)
            (recur (inc attempt) health))
          (or health last-health))))))

(defn launch-node-process! [cfg node]
  (let [id (node-id cfg node)
        port (node-port node)
        log-file (io/file (:data-dir cfg) (str "node-" id ".log"))
        data-dir (io/file (:data-dir cfg) (str "node-" id))
        pb (ProcessBuilder.
            (into-array String [(:bin cfg)
                                "-id" (str id)
                                "-listen" (str ":" port)
                                "-data" (.getPath data-dir)
                                "-peers" (:peers cfg)]))]
    (.mkdirs data-dir)
    (.redirectOutput pb (ProcessBuilder$Redirect/appendTo log-file))
    (.redirectError pb (ProcessBuilder$Redirect/appendTo log-file))
    (str (.pid (.start pb)))))

(defn start-node! [cfg node]
  (let [pid (read-pid cfg node)]
    (if (pid-alive? pid)
      (let [health (wait-node-health cfg node)]
        (if (:healthy health)
          (merge health {:node node :pid pid :action :already-running})
          (merge health {:node node :pid pid :action :start-failed})))
      (let [new-pid (launch-node-process! cfg node)
            health (wait-node-health cfg node)]
        (write-pid! cfg node new-pid)
        (if (:healthy health)
          (merge health {:node node :pid new-pid :action :started})
          (merge health {:node node :pid new-pid :action :start-failed}))))))

(defn local-data-dir [cfg node]
  (io/file (:data-dir cfg) (str "node-" (node-id cfg node))))

(defn local-storage-backup-dir [cfg node]
  (io/file (:data-dir cfg) (str "node-" (node-id cfg node) ".removed")))

(defn local-rm-rf! [path]
  (sh "rm" "-rf" (.getPath (io/file path))))

(defn local-move-if-present! [source dest]
  (when (.exists (io/file source))
    (local-rm-rf! dest)
    (sh "mv" (.getPath (io/file source)) (.getPath (io/file dest)))))

(defn remove-local-storage! [cfg node]
  (let [stop (stop-node! cfg node)
        data-dir (local-data-dir cfg node)
        backup-dir (local-storage-backup-dir cfg node)]
    (local-move-if-present! data-dir backup-dir)
    (let [start (start-node! cfg node)]
      {:node node :action :storage-removed :stop stop :start start})))

(defn restore-local-storage! [cfg node]
  (let [data-dir (local-data-dir cfg node)
        backup-dir (local-storage-backup-dir cfg node)]
    (if (.exists backup-dir)
      (let [stop (stop-node! cfg node)]
        (local-rm-rf! data-dir)
        (local-move-if-present! backup-dir data-dir)
        (let [start (start-node! cfg node)]
          {:node node :action :storage-restored :stop stop :start start}))
      {:node node :action :storage-unchanged})))

(defn local-destructive-storage-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this _ op]
      (try
        (case (:f op)
          :remove-storage (assoc op :type :info :value (remove-local-storage! cfg (:value op)))
          :restore-storage (assoc op :type :info :value (restore-local-storage! cfg (:value op)))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "local destructive storage nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this _]
      (doseq [node (:nodes cfg)]
        (try
          (restore-local-storage! cfg node)
          (catch Exception e
            (warn e "local destructive storage nemesis repair failed"))))
      this)))

(defn local-restart-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this _ op]
      (try
        (case (:f op)
          :kill-node (assoc op :type :info :value (stop-node! cfg (:value op)))
          :restart-node (assoc op :type :info :value (start-node! cfg (:value op)))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "local restart nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this _]
      (doseq [node (:nodes cfg)]
        (try
          (start-node! cfg node)
          (catch Exception e
            (warn e "local restart nemesis repair failed"))))
      this)))

(defn local-restart-generator [nodes]
  (mapcat (fn [node]
            [(gen/sleep 1)
             {:type :info :f :kill-node :value node}
             (gen/sleep 1)
             {:type :info :f :restart-node :value node}])
          nodes))

(defn transport-fault-request! [node from to drop?]
  (let [resp (http/post (str (endpoint {} node) "/faults/transport")
                        {:body (json/write-str {"from" from "to" to "drop" drop?})
                         :content-type :json
                         :throw-exceptions false})]
    {:node node :from from :to to :drop drop? :status (:status resp)}))

(defn transport-isolation-requests [cfg node]
  (let [target (node-id cfg node)]
    (vec
     (for [peer (:nodes cfg)
           :when (not= peer node)
           controller (:nodes cfg)
           :let [peer-id (node-id cfg peer)]
           [from to] [[target peer-id] [peer-id target]]]
       {:controller controller :from from :to to}))))

(defn set-transport-isolation! [cfg node drop?]
  (mapv (fn [{:keys [controller from to]}]
          (transport-fault-request! (http-node cfg controller) from to drop?))
        (transport-isolation-requests cfg node)))

(defn storage-fault-request! [node fail?]
  (let [resp (http/post (str (endpoint {} node) "/faults/storage")
                        {:body (json/write-str {"fail" fail?})
                         :content-type :json
                         :throw-exceptions false})]
    {:node node :fail fail? :status (:status resp)}))

(defn local-transport-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this _ op]
      (try
        (case (:f op)
          :isolate-node (assoc op :type :info :value (set-transport-isolation! cfg (:value op) true))
          :heal-node (assoc op :type :info :value (set-transport-isolation! cfg (:value op) false))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "local transport nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this _]
      (doseq [node (:nodes cfg)]
        (try
          (set-transport-isolation! cfg node false)
          (catch Exception e
            (warn e "local transport nemesis repair failed"))))
      this)))

(defn local-transport-generator [nodes]
  (mapcat (fn [node]
            [(gen/sleep 1)
             {:type :info :f :isolate-node :value node}
             (gen/sleep 1)
             {:type :info :f :heal-node :value node}])
          nodes))

(defn local-storage-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this _ op]
      (try
        (case (:f op)
          :fail-storage (assoc op :type :info :value (storage-fault-request! (http-node cfg (:value op)) true))
          :heal-storage (assoc op :type :info :value (storage-fault-request! (http-node cfg (:value op)) false))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "local storage nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this _]
      (doseq [node (:nodes cfg)]
        (try
          (storage-fault-request! (http-node cfg node) false)
          (catch Exception e
            (warn e "local storage nemesis repair failed"))))
      this)))

(defn local-storage-generator [nodes]
  (mapcat (fn [node]
            [(gen/sleep 1)
             {:type :info :f :fail-storage :value node}
             (gen/sleep 1)
             {:type :info :f :heal-storage :value node}])
          nodes))

(defn destructive-storage-generator [nodes]
  (mapcat (fn [node]
            [(gen/sleep 1)
             {:type :info :f :remove-storage :value node}
             (gen/sleep 1)
             {:type :info :f :restore-storage :value node}])
          nodes))

(defn local-fault-generator [cfg]
  (case (:faults cfg)
    "transport" (local-transport-generator (:nodes cfg))
    "storage" (local-storage-generator (:nodes cfg))
    "destructive-storage" (destructive-storage-generator (:nodes cfg))
    (local-restart-generator (:nodes cfg))))

(defn local-fault-nemesis [cfg]
  (case (:faults cfg)
    "transport" (local-transport-nemesis cfg)
    "storage" (local-storage-nemesis cfg)
    "destructive-storage" (local-destructive-storage-nemesis cfg)
    (local-restart-nemesis cfg)))

(defn remote-config [opts]
  (let [nodes (:nodes opts)
        bin (fault-env "BIN")
        http-port (parse-env-long "HTTP_PORT" 8080)
        base {:faults (fault-env "FAULTS")
              :bin bin
              :remote-dir (or (fault-env "REMOTE_DIR") "/tmp/moreconsensus-jepsen")
              :http-port http-port
              :nodes nodes
              :node-ids (indexed-node-ids nodes)}]
    (when (and (env-enabled? "REMOTE") (seq bin) (seq nodes))
      (assoc base :peers (peer-spec base)))))

(defn fault-enabled-config [cfg]
  (when (and cfg (contains? supported-faults (:faults cfg)))
    cfg))

(defn remote-bin-path [cfg]
  (str (:remote-dir cfg) "/kvnode"))

(defn remote-node-data-dir [cfg node]
  (str (:remote-dir cfg) "/node-" (node-id cfg node)))

(defn remote-node-backup-dir [cfg node]
  (str (:remote-dir cfg) "/node-" (node-id cfg node) ".removed"))

(defn remote-pid-file [cfg node]
  (str (:remote-dir cfg) "/node-" (node-id cfg node) ".pid"))

(defn remote-log-file [cfg node]
  (str (:remote-dir cfg) "/node-" (node-id cfg node) ".log"))

(defn stop-remote-node! [cfg node]
  (cu/stop-daemon! (remote-pid-file cfg node))
  {:node node :action :stopped})

(defn start-remote-node! [cfg node]
  (let [data-dir (remote-node-data-dir cfg node)
        result (do
                 (control/exec :mkdir :-p data-dir)
                 (cu/start-daemon!
                  {:logfile (remote-log-file cfg node)
                   :pidfile (remote-pid-file cfg node)
                   :chdir (:remote-dir cfg)}
                  (remote-bin-path cfg)
                  "-id" (str (node-id cfg node))
                  "-listen" (str ":" (node-http-port cfg node))
                  "-data" data-dir
                  "-peers" (:peers cfg)))
        health (wait-node-health cfg node)]
    (merge health {:node node :action result})))

(defn remote-setup-node! [cfg node]
  (control/exec :mkdir :-p (:remote-dir cfg))
  (control/upload (:bin cfg) (remote-bin-path cfg))
  (control/exec :chmod :+x (remote-bin-path cfg))
  (stop-remote-node! cfg node)
  (control/exec :rm :-rf (remote-node-data-dir cfg node) (remote-node-backup-dir cfg node))
  (start-remote-node! cfg node))

(defn remote-teardown-node! [cfg node]
  (stop-remote-node! cfg node)
  (control/exec :rm :-rf (remote-node-data-dir cfg node) (remote-node-backup-dir cfg node)))

(defn remote-move-if-present! [source dest]
  (when (cu/exists? source)
    (control/exec :rm :-rf dest)
    (control/exec :mv source dest)))

(defn remove-remote-storage! [cfg node]
  (let [stop (stop-remote-node! cfg node)
        data-dir (remote-node-data-dir cfg node)
        backup-dir (remote-node-backup-dir cfg node)]
    (remote-move-if-present! data-dir backup-dir)
    (let [start (start-remote-node! cfg node)]
      {:node node :action :storage-removed :stop stop :start start})))

(defn restore-remote-storage! [cfg node]
  (let [data-dir (remote-node-data-dir cfg node)
        backup-dir (remote-node-backup-dir cfg node)]
    (if (cu/exists? backup-dir)
      (let [stop (stop-remote-node! cfg node)]
        (control/exec :rm :-rf data-dir)
        (remote-move-if-present! backup-dir data-dir)
        (let [start (start-remote-node! cfg node)]
          {:node node :action :storage-restored :stop stop :start start}))
      {:node node :action :storage-unchanged})))

(defrecord KVNodeDB [cfg]
  db/DB
  (setup! [_ test node]
    (remote-setup-node! (merge cfg (select-keys test [:http-port])) node))
  (teardown! [_ test node]
    (when-not (:leave-db-running? test)
      (remote-teardown-node! (merge cfg (select-keys test [:http-port])) node)))

  db/Kill
  (kill! [_ test node]
    (stop-remote-node! (merge cfg (select-keys test [:http-port])) node))
  (start! [_ test node]
    (start-remote-node! (merge cfg (select-keys test [:http-port])) node))

  db/LogFiles
  (log-files [_ _ node]
    {(remote-log-file cfg node) (str "kvnode-" (node-id cfg node) ".log")}))

(defn kvnode-db [cfg]
  (->KVNodeDB cfg))

(defn remote-node-op! [test node f]
  (get (control/on-nodes test [node] (fn [_ seen-node] (f seen-node))) node))

(defn remote-restart-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this test op]
      (try
        (case (:f op)
          :kill-node (assoc op :type :info :value (remote-node-op! test (:value op) #(stop-remote-node! cfg %)))
          :restart-node (assoc op :type :info :value (remote-node-op! test (:value op) #(start-remote-node! cfg %)))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "remote restart nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this test]
      (control/on-nodes test (:nodes cfg)
                        (fn [_ node]
                          (try
                            (start-remote-node! cfg node)
                            (catch Exception e
                              (warn e "remote restart nemesis repair failed")))))
      this)))

(defn remote-destructive-storage-nemesis [cfg]
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this test op]
      (try
        (case (:f op)
          :remove-storage (assoc op :type :info :value (remote-node-op! test (:value op) #(remove-remote-storage! cfg %)))
          :restore-storage (assoc op :type :info :value (remote-node-op! test (:value op) #(restore-remote-storage! cfg %)))
          (assoc op :type :info :value :unknown-nemesis-op))
        (catch Exception e
          (warn e "remote destructive storage nemesis operation failed")
          (assoc op :type :info :value {:error (.getMessage e)}))))
    (teardown! [this test]
      (control/on-nodes test (:nodes cfg)
                        (fn [_ node]
                          (try
                            (restore-remote-storage! cfg node)
                            (catch Exception e
                              (warn e "remote destructive storage nemesis repair failed")))))
      this)))

(defn remote-fault-nemesis [cfg]
  (case (:faults cfg)
    "restart" (remote-restart-nemesis cfg)
    "transport" (local-transport-nemesis cfg)
    "storage" (local-storage-nemesis cfg)
    "destructive-storage" (remote-destructive-storage-nemesis cfg)
    (local-restart-nemesis cfg)))


(defn ok-status? [status]
  (contains? #{200 201 202 204} status))

(defn indeterminate-status? [status]
  (and (integer? status) (<= 500 status)))

(defn mutation-result [op resp]
  (let [status (:status resp)]
    (cond
      (ok-status? status) (assoc op :type :ok)
      (indeterminate-status? status) (assoc op :type :info :error status)
      :else (assoc op :type :fail :error status))))

(defn txn-group-for [n]
  (nth txn-group-ids (mod n (count txn-group-ids))))

(defn scan-key-for [n]
  (nth scan-keys (mod n (count scan-keys))))

(defn txn-body [group value]
  (let [keys (or (get txn-keys-by-group group)
                 (throw (ex-info "unknown transaction group" {:group group})))]
    (json/write-str
     (mapv (fn [k] {"key" k "value" (pr-str value)}) keys))))

(defn txn-delete-body [group]
  (let [keys (or (get txn-keys-by-group group)
                 (throw (ex-info "unknown transaction group" {:group group})))]
    (json/write-str
     (mapv (fn [k] {"key" k "delete" true}) keys))))

(defn parse-rows [body]
  (json/read-str body :key-fn keyword))

(defn row-values [rows]
  (into {} (map (fn [row]
                  [(:key row) (edn/read-string (:value row))])
                rows)))

(defn scan-rows [base params]
  (try
    (let [query-params (into {} (remove (comp nil? val)) params)
          resp (http/get (str base "/scan")
                         {:query-params query-params
                          :throw-exceptions false})]
      (if (= 200 (:status resp))
        [:ok (parse-rows (:body resp))]
        [:fail (:status resp)]))
    (catch Exception e
      [:fail (.getMessage e)])))

(defn scan-map
  ([base prefix] (scan-map base prefix nil))
  ([base prefix barrier]
   (let [[status rows] (scan-rows base {"prefix" prefix
                                        "barrier" barrier})]
     (if (= :ok status)
       [:ok (row-values rows)]
       [:fail rows]))))

(defn advanced-scan [base reverse?]
  (let [[status rows] (scan-rows base {"prefix" scan-prefix
                                       "limit" (str scan-limit)
                                       "reverse" (when reverse? "true")
                                       "barrier" scan-barrier})]
    (if (= :ok status)
      [:ok rows]
      [:fail rows])))

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
            (mutation-result op resp))
          :read
          (let [resp (http/get (str base "/kv/" k)
                               {:throw-exceptions false})]
            (cond
              (= 200 (:status resp)) (assoc op :type :ok :value (edn/read-string (:body resp)))
              (= 404 (:status resp)) (assoc op :type :ok :value nil)
              :else (assoc op :type :fail :error (:status resp))))
          :delete
          (let [resp (http/delete (str base "/kv/" k)
                                  {:throw-exceptions false})]
            (mutation-result op resp))
          :txn-write
          (let [{:keys [group value]} (:value op)
                resp (http/post (str base "/txn")
                                {:body (txn-body group value)
                                 :content-type :json
                                 :throw-exceptions false})]
            (mutation-result op resp))
          :txn-delete
          (let [{:keys [group]} (:value op)
                resp (http/post (str base "/txn")
                                {:body (txn-delete-body group)
                                 :content-type :json
                                 :throw-exceptions false})]
            (mutation-result op resp))
          :scan-write
          (let [{:keys [key value]} (:value op)
                resp (http/put (str base "/kv/" key)
                               {:body (pr-str value)
                                :throw-exceptions false})]
            (mutation-result op resp))
          :scan-delete
          (let [{:keys [key]} (:value op)
                resp (http/delete (str base "/kv/" key)
                                  {:throw-exceptions false})]
            (mutation-result op resp))
          :scan-forward
          (let [[status rows] (advanced-scan base false)]
            (if (= :ok status)
              (assoc op :type :ok :value rows)
              (assoc op :type :fail :error rows)))
          :scan-reverse
          (let [[status rows] (advanced-scan base true)]
            (if (= :ok status)
              (assoc op :type :ok :value rows)
              (assoc op :type :fail :error rows)))
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

(defn normalize-register-op [op]
  (if (= :delete (:f op))
    (assoc op :f :write :value nil)
    op))

(defn register-linearizable-checker []
  (let [linear (checker/linearizable {:model (model/register)})]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check linear test (mapv normalize-register-op (filterv #(contains? register-ops (:f %)) history)) opts)))))

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

(defn ordered-ascending? [xs]
  (every? (fn [[a b]] (not (pos? (compare a b)))) (partition 2 1 xs)))

(defn ordered-descending? [xs]
  (every? (fn [[a b]] (not (neg? (compare a b)))) (partition 2 1 xs)))

(defn bad-scan-op [op]
  (let [rows (:value op)
        keys (mapv :key rows)]
    (cond
      (> (count rows) scan-limit) (assoc op :bad-scan :limit)
      (some #(not (str/starts-with? % scan-prefix)) keys) (assoc op :bad-scan :prefix)
      (and (= :scan-forward (:f op)) (not (ordered-ascending? keys))) (assoc op :bad-scan :order)
      (and (= :scan-reverse (:f op)) (not (ordered-descending? keys))) (assoc op :bad-scan :order))))

(defn advanced-scan-checker []
  (reify checker/Checker
    (check [_ _ history _]
      (let [reads (filter #(and (= :ok (:type %)) (contains? #{:scan-forward :scan-reverse} (:f %))) history)
            bad (vec (keep bad-scan-op reads))]
        {:valid? (empty? bad)
         :checked (count reads)
         :bad-count (count bad)
         :bad (take 5 bad)}))))

(defn client-workload-generator []
  (gen/limit 90
             (gen/mix [(map (fn [x] {:type :invoke :f :write :value x}) (range))
                       (gen/repeat {:type :invoke :f :read :value nil})
                       (gen/repeat {:type :invoke :f :delete :value nil})
                       (map (fn [x] {:type :invoke :f :scan-write :value {:key (scan-key-for x) :value x}}) (range))
                       (map (fn [x] {:type :invoke :f :scan-delete :value {:key (scan-key-for x)}}) (range))
                       (gen/repeat {:type :invoke :f :scan-forward :value nil})
                       (gen/repeat {:type :invoke :f :scan-reverse :value nil})
                       (map (fn [x] {:type :invoke :f :txn-write :value {:group (txn-group-for x) :value x}}) (range))
                       (map (fn [x] {:type :invoke :f :txn-delete :value {:group (txn-group-for x)}}) (range))
                       (gen/repeat {:type :invoke :f :txn-read :value nil})])))

(defn workload
  ([] (workload nil))
  ([fault-cfg]
   (let [client-gen (client-workload-generator)]
     {:client (->KVClient nil)
      :generator (if fault-cfg
                   (gen/clients client-gen (local-fault-generator fault-cfg))
                   (gen/clients client-gen))
     :checker (checker/compose {:linearizable (register-linearizable-checker)
                                :scan-shape (advanced-scan-checker)
                                :txn-atomic (txn-atomic-checker)
                                :timeline (timeline/html)})})))

(defn moreconsensus-test [opts]
  (let [local-fault-cfg (local-fault-config opts)
        remote-cfg (remote-config opts)
        remote-fault-cfg (fault-enabled-config remote-cfg)
        fault-cfg (or remote-fault-cfg local-fault-cfg)]
    (merge tests/noop-test
           opts
           (workload fault-cfg)
           {:name "moreconsensus-epaxos-kv"}
           (when remote-cfg
             {:db (kvnode-db remote-cfg)
              :http-port (:http-port remote-cfg)})
           (when fault-cfg
             {:nemesis (if remote-fault-cfg
                         (remote-fault-nemesis fault-cfg)
                         (local-fault-nemesis fault-cfg))}))))

(defn -main [& args]
  (cli/run! (cli/single-test-cmd {:test-fn moreconsensus-test}) args))
