(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: multi-partition log correctness with rebalancer chaos.
   - :produce appends a uniquely identified value to a random partition
   - :consume reads the next unconsumed value from a partition (per-partition offset)
   - Checker validates: per-partition ordering, global completeness (no lost/dupes)"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [generator :as gen]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.barka.client :as barka]
            [jepsen.barka.db :as db]
            [jepsen.barka.nemesis :as barka-nemesis])
  (:import (java.util UUID)))

(defn log-checker
  "Checks that every produced value is consumed exactly once, in offset order
   within each partition. Uses server-assigned offsets for ordering."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ok-produces (->> history
                             (filter #(and (= :produce (:f %)) (= :ok (:type %)))))
            ok-consumes (->> history
                             (filter #(and (= :consume (:f %)) (= :ok (:type %)))))
            ;; Per-partition analysis
            produces-by-p (group-by :partition ok-produces)
            consumes-by-p (group-by :partition ok-consumes)
            partition-results
            (into {}
              (map (fn [[p produces]]
                     (let [expected (->> produces
                                        (sort-by :offset)
                                        (mapv :value))
                           consumed (mapv :value (get consumes-by-p p []))
                           dups     (- (count consumed) (count (distinct consumed)))]
                       [p {:produced   (count produces)
                           :consumed   (count consumed)
                           :duplicates dups
                           :ordered?   (= consumed
                                         (vec (take (count consumed) expected)))}]))
                   produces-by-p))
            ;; Global completeness
            all-produced (set (map :value ok-produces))
            all-consumed (mapv :value ok-consumes)
            duplicates   (- (count all-consumed) (count (distinct all-consumed)))
            lost         (vec (remove (set all-consumed) all-produced))
            unexpected   (vec (remove all-produced all-consumed))
            all-ordered? (every? :ordered? (vals partition-results))
            valid?       (and (empty? lost)
                              (empty? unexpected)
                              (zero? duplicates)
                              all-ordered?)]
        {:valid?      valid?
         :partitions  partition-results
         :produced    (count all-produced)
         :consumed    (count (distinct all-consumed))
         :duplicates  duplicates
         :lost        (count lost)
         :lost-values (take 10 lost)
         :unexpected  (count unexpected)
         :in-order?   all-ordered?}))))

(def next-value (atom 0))

(defn barka-client
  "A Jepsen client for barka's distributed log.
   Each partition has its own consume-offset atom."
  [conn]
  (reify client/Client
    (open! [this test node]
      (barka-client (barka/open "127.0.0.1" (db/jepsen-gateway-port-for node))))

    (setup! [this test])

    (invoke! [this test op]
      (try
        (case (:f op)
          :produce
          (let [offset (barka/produce! conn "default" (:partition op) (:value op))]
            (assoc op :type :ok :offset offset))

          :consume
          (let [p        (:partition op)
                off-atom (get (:consume-offsets test) p)
                off      @off-atom
                resp     (barka/consume! conn "default" p off 1)]
            (cond
              (empty? (:values resp))
              (assoc op :type :fail :error :empty)

              (compare-and-set! off-atom off (:next-offset resp))
              (assoc op :type :ok :value (parse-long (first (:values resp))))

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
  (let [n              (get opts :num-nodes 3)
        num-partitions (get opts :num-partitions 4)
        node-names     (mapv #(str "n" (inc %)) (range n))
        consume-offsets (into {} (map (fn [p] [p (atom 0)]) (range num-partitions)))
        run-id         (str (UUID/randomUUID))
        s3-prefix      (str "jepsen/" run-id)
        opts           (assoc opts :run-id run-id :s3-prefix s3-prefix)]
    (merge tests/noop-test
           opts
           {:name            "barka"
            :concurrency     (* 2 n)
            :consume-offsets consume-offsets
            :num-partitions  num-partitions
            :db              (db/db opts)
            :client          (barka-client nil)
            :nemesis         (barka-nemesis/rebalancer-nemesis opts)
            :checker         (checker/compose
                               {:log      (log-checker)
                                :timeline (timeline/html)})
            :generator       (gen/phases
                               ;; Phase 1: produce + consume with rebalancer chaos
                               (gen/any
                                 (->> (gen/mix
                                        [(fn [_ _]
                                           {:type      :invoke
                                            :f         :produce
                                            :value     (swap! next-value inc)
                                            :partition (rand-int num-partitions)})
                                         (fn [_ _]
                                           {:type      :invoke
                                            :f         :consume
                                            :partition (rand-int num-partitions)})])
                                      (gen/stagger 1/50)
                                      (gen/clients)
                                      (gen/time-limit 30))
                                 (->> (fn [_ _] {:type :invoke :f :rebalance})
                                      (gen/stagger 7)
                                      (gen/nemesis)
                                      (gen/time-limit 30)))
                               ;; Phase 2: drain all partitions
                               (gen/log "Draining...")
                               (->> (fn [_ _]
                                      {:type      :invoke
                                       :f         :consume
                                       :partition (rand-int num-partitions)})
                                    (gen/stagger 1/100)
                                    (gen/clients)
                                    (gen/time-limit 15)))
            :nodes           node-names
            :ssh             {:dummy? true}})))

(def cli-opts
  [[nil "--bin-dir DIR" "Directory containing produce-node, consume-node, jepsen-gateway, rebalancer binaries"]
   [nil "--num-nodes NUM" "Number of barka nodes"
    :default 3
    :parse-fn parse-long
    :validate [#(<= 1 % 10) "Must be between 1 and 10"]]
   [nil "--num-partitions NUM" "Number of partitions"
    :default 4
    :parse-fn parse-long
    :validate [#(<= 1 % 64) "Must be between 1 and 64"]]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run!
    (cli/single-test-cmd {:test-fn  barka-test
                           :opt-spec cli-opts})
    args))
