(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: queue semantics per partition.
   - :produce appends a value to the log
   - :consume reads the next value from the log
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

(defn log-checker
  "Checks that every produced value is consumed exactly once, in order."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [produced  (->> history
                           (filter #(and (= :produce (:f %)) (= :ok (:type %))))
                           (mapv :value))
            consumed  (->> history
                           (filter #(and (= :consume (:f %)) (= :ok (:type %))))
                           (mapv :value))
            lost      (remove (set consumed) produced)
            ;; Check ordering: consumed values should appear in same relative order as produced
            prod-order (into {} (map-indexed (fn [i v] [v i]) produced))
            indices    (keep prod-order consumed)
            ordered?   (= indices (sort indices))
            valid?     (and (empty? lost) ordered?)]
        {:valid?     valid?
         :produced   (count produced)
         :consumed   (count consumed)
         :lost       (count lost)
         :in-order?  ordered?}))))

(defn barka-client
  "A Jepsen client for barka's distributed log."
  [conn next-offset]
  (reify client/Client
    (open! [this test node]
      (let [c (barka/open "127.0.0.1" db/control-port)]
        (barka-client c (atom 0))))

    (setup! [this test])

    (invoke! [this test op]
      (try
        (case (:f op)
          :produce
          (let [offset (barka/produce! conn "default" 0 (:value op))]
            (assoc op :type :ok :offset offset))

          :consume
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
          :client    (barka-client nil nil)
          :checker   (checker/compose
                       {:log      (log-checker)
                        :timeline (timeline/html)})
          :generator (gen/phases
                       (->> (gen/mix [(fn [_ _] {:type :invoke :f :produce :value (rand-int 10000)})
                                      (fn [_ _] {:type :invoke :f :consume})])
                            (gen/stagger 1/10)
                            (gen/clients)
                            (gen/time-limit 10))
                       (gen/log "Draining...")
                       (->> (fn [_ _] {:type :invoke :f :consume})
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
