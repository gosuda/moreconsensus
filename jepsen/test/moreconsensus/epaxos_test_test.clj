(ns moreconsensus.epaxos-test-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [moreconsensus.epaxos-test :as epaxos]))

(deftest txn-body-encodes-selected-group-as-json
  (testing "writes one EDN value to every key in the chosen transaction group"
    (let [value {:quoted "a\"b" :items [:x 1]}
          rows (json/read-str (epaxos/txn-body :tx-b value) :key-fn keyword)]
      (is (= (mapv (fn [k] {:key k :value (pr-str value)})
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
               (epaxos/scan-values "http://node")))))))

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
             (:bad-groups bad-op))))))
