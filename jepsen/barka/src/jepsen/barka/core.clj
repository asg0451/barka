(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: single-partition log correctness.
   - :produce appends a uniquely identified value to the log
   - :consume reads the next unconsumed value from the log (shared offset)
   - Checker validates: every produced value consumed exactly once, in offset order."
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
  "Checks that every produced value is consumed exactly once, in offset order.
   Uses server-assigned offsets from produce responses to determine expected order
   rather than relying on history (acknowledgement-time) ordering."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [produces   (->> history
                            (filter #(and (= :produce (:f %)) (= :ok (:type %))))
                            vec)
            consumed   (->> history
                            (filter #(and (= :consume (:f %)) (= :ok (:type %))))
                            (mapv :value))
            produced   (mapv :value produces)
            ;; Sort produces by server-assigned offset, not by position in history.
            ;; History order reflects ack time which can diverge from offset order
            ;; under concurrent producers.
            expected   (->> produces
                            (sort-by :offset)
                            (mapv :value))
            duplicates (- (count consumed) (count (distinct consumed)))
            lost       (vec (remove (set consumed) produced))
            unexpected (vec (remove (set produced) consumed))
            ordered?   (= consumed (vec (take (count consumed) expected)))
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
            (cond
              (empty? values)
              (assoc op :type :fail :error :empty)

              (compare-and-set! consume-offset off (inc off))
              (assoc op :type :ok :value (parse-long (first values)))

              :else
              (assoc op :type :fail :error :cas-failed))))
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
            :concurrency    1
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
                                   (gen/stagger 1/50)
                                   (gen/clients)
                                   (gen/time-limit 10)))
            :nodes          ["n1"]
            :ssh            {:dummy? true}})))

(def cli-opts
  [[nil "--barka-bin PATH" "Path to barka binary"
    :default "barka"]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run!
    (cli/single-test-cmd {:test-fn  barka-test
                          :opt-spec cli-opts})
    args))
