(ns moreconsensus.epaxos-test-test
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [moreconsensus.epaxos-test :as epaxos]))

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
