(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: single-partition log correctness.
   - :produce appends a uniquely identified value to the log
   - :consume reads the next unconsumed value from the log (shared offset)
   - Checker validates: every produced value consumed exactly once, in order."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.barka.client :as barka]
            [jepsen.barka.db :as db]))

(defn log-checker
  "Checks that every produced value is consumed exactly once, in produce order."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [produced   (->> history
                            (filter #(and (= :produce (:f %)) (= :ok (:type %))))
                            (mapv :value))
            consumed   (->> history
                            (filter #(and (= :consume (:f %)) (= :ok (:type %))))
                            (mapv :value))
            duplicates (- (count consumed) (count (distinct consumed)))
            lost       (vec (remove (set consumed) produced))
            unexpected (vec (remove (set produced) consumed))
            ;; Check ordering: consumed should be a subsequence of produced
            prod-idx   (zipmap produced (range))
            indices    (keep prod-idx consumed)
            ordered?   (= (seq indices) (seq (sort indices)))
            valid?     (and (empty? lost)
                            (empty? unexpected)
                            (zero? duplicates)
                            ordered?)]
        {:valid?      valid?
         :produced    (count produced)
         :consumed    (count consumed)
         :duplicates  duplicates
         :lost        (count lost)
         :lost-values (take 10 lost)
         :unexpected  (count unexpected)
         :in-order?   ordered?}))))

(def next-value (atom 0))

(defn barka-client
  "A Jepsen client for barka's distributed log.
   All clients share a consume-offset atom that only advances on successful reads."
  [conn consume-offset]
  (reify client/Client
    (open! [this test node]
      (barka-client (barka/open "127.0.0.1" db/control-port)
                    (:consume-offset test)))

    (setup! [this test])

    (invoke! [this test op]
      (try
        (case (:f op)
          :produce
          (let [offset (barka/produce! conn "default" 0 (:value op))]
            (assoc op :type :ok :offset offset))

          :consume
          (let [off    @consume-offset
                values (barka/consume! conn "default" 0 off 1)]
            (if (seq values)
              (do (compare-and-set! consume-offset off (inc off))
                  (assoc op :type :ok :value (parse-long (first values))))
              (assoc op :type :fail :error :empty))))
        (catch Exception e
          (assoc op :type :info :error (.getMessage e)))))

    (teardown! [this test])

    (close! [this test]
      (when conn
        (barka/close! conn)))))

(defn barka-test
  "Constructs a Jepsen test map for barka."
  [opts]
  (reset! next-value 0)
  (let [consume-offset (atom 0)]
    (merge tests/noop-test
           opts
           {:name           "barka"
            :consume-offset consume-offset
            :db             (db/db opts)
            :client         (barka-client nil nil)
            :checker        (checker/compose
                              {:log      (log-checker)
                               :timeline (timeline/html)})
            :generator      (gen/phases
                              (->> (gen/mix [(fn [_ _]
                                              {:type :invoke
                                               :f :produce
                                               :value (swap! next-value inc)})
                                             (fn [_ _]
                                               {:type :invoke :f :consume})])
                                   (gen/stagger 1/10)
                                   (gen/clients)
                                   (gen/time-limit 10))
                              (gen/log "Draining...")
                              (->> (fn [_ _] {:type :invoke :f :consume})
                                   (gen/stagger 1/10)
                                   (gen/clients)
                                   (gen/time-limit 5)))
            :nodes          ["n1"]
            :ssh            {:dummy? true}})))

(def cli-opts
  [[nil "--barka-bin PATH" "Path to barka binary"
    :default "barka"]
   [nil "--project-root PATH" "Root of the barka project (where docker-compose.yml lives)"]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run!
    (cli/single-test-cmd {:test-fn  barka-test
                          :opt-spec cli-opts})
    args))
