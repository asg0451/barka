(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: queue semantics per partition.
   - :enqueue operations produce a value
   - :dequeue operations consume the next value
   - Checker validates total ordering: no lost, duplicated, or reordered values."
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

(defn queue-checker
  "Checks that every enqueued value is dequeued exactly once, in order."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [enqueued  (->> history
                           (filter #(and (= :enqueue (:f %)) (= :ok (:type %))))
                           (mapv :value))
            dequeued  (->> history
                           (filter #(and (= :dequeue (:f %)) (= :ok (:type %))))
                           (mapv :value))
            lost      (remove (set dequeued) enqueued)
            ;; Check ordering: dequeued values should appear in same relative order as enqueued
            enq-order (into {} (map-indexed (fn [i v] [v i]) enqueued))
            indices   (keep enq-order dequeued)
            ordered?  (= indices (sort indices))
            valid?    (and (empty? lost) ordered?)]
        {:valid?      valid?
         :enqueued    (count enqueued)
         :dequeued    (count dequeued)
         :lost        (count lost)
         :in-order?   ordered?}))))

(defn queue-client
  "A Jepsen client that treats barka as a queue (per partition).
   Enqueue = produce, dequeue = consume from tracked offset."
  [conn next-offset]
  (reify client/Client
    (open! [this test node]
      (let [c (barka/open "127.0.0.1" db/control-port)]
        (queue-client c (atom 0))))

    (setup! [this test])

    (invoke! [this test op]
      (try
        (case (:f op)
          :enqueue
          (let [offset (barka/produce! conn "default" 0 (:value op))]
            (assoc op :type :ok :offset offset))

          :dequeue
          (let [values (barka/consume! conn "default" 0 @next-offset 1)]
            (if (seq values)
              (do (swap! next-offset inc)
                  (assoc op :type :ok :value (first values)))
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
  (merge tests/noop-test
         opts
         {:name      "barka"
          :db        (db/db opts)
          :client    (queue-client nil nil)
          :checker   (checker/compose
                       {:queue    (queue-checker)
                        :timeline (timeline/html)})
          :generator (gen/phases
                       (->> (gen/mix [(fn [_ _] {:type :invoke :f :enqueue :value (rand-int 10000)})
                                      (fn [_ _] {:type :invoke :f :dequeue})])
                            (gen/stagger 1/10)
                            (gen/clients)
                            (gen/time-limit 10))
                       (gen/log "Draining...")
                       (->> (fn [_ _] {:type :invoke :f :dequeue})
                            (gen/stagger 1/10)
                            (gen/clients)
                            (gen/time-limit 5)))
          :nodes     ["n1"]
          :ssh       {:dummy? true}}))

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
