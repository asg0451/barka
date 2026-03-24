(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: multi-partition log correctness with rebalancer chaos.
   - :produce appends values to a random partition (single or batch)
   - :consume reads the next unconsumed value from a partition (per-partition offset)
   - Checker validates:
     1. Intra-batch ordering: offsets within a batch are strictly increasing
     2. Real-time ordering: if produce A completes before B is invoked (same
        partition), max(A.offsets) < min(B.offsets)
     3. Consumed-offset ordering and global completeness (no lost/dupes)"
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

(defn- normalize-produce
  "Normalizes a completed produce op to always have :values and :offsets vecs."
  [op]
  (if (:values op)
    op
    (assoc op :values [(:value op)] :offsets [(:offset op)])))

(defn log-checker
  "Checks ordering invariants and completeness for the distributed log.

   Invariants per partition:
   1. Intra-batch: each batch produce's offsets are strictly increasing
   2. Real-time: if produce A :ok precedes produce B :invoke in history,
      max(A.offsets) < min(B.offsets)
   3. Consumed values appear in server-assigned offset order
   4. Completeness: no lost values, no unexpected values, no duplicates"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [;; Ensure every op has an :index for real-time ordering
            history (if (:index (first history))
                      history
                      (map-indexed (fn [i op] (assoc op :index i)) history))

            ok-produces (->> history
                             (filter #(and (= :produce (:f %)) (= :ok (:type %))))
                             (mapv normalize-produce))
            ok-consumes (->> history
                             (filter #(and (= :consume (:f %)) (= :ok (:type %)))))

            ;; --- Invariant 1: Intra-batch monotonicity ---
            batch-violations
            (->> ok-produces
                 (filter #(> (count (:offsets %)) 1))
                 (remove (fn [op]
                           (every? (fn [[a b]] (< a b))
                                   (partition 2 1 (:offsets op)))))
                 (mapv (fn [op] {:values (:values op) :offsets (:offsets op)})))

            ;; --- Invariant 2: Real-time ordering ---
            ;; Pair each ok-produce with its invoke index by walking full history
            produce-pairs
            (:pairs
              (reduce
                (fn [state op]
                  (cond
                    (and (= :produce (:f op)) (= :invoke (:type op)))
                    (assoc-in state [:last-invoke (:process op)] (:index op))

                    (and (= :produce (:f op)) (= :ok (:type op)))
                    (let [inv-idx (get-in state [:last-invoke (:process op)])
                          norm    (normalize-produce op)]
                      (update state :pairs conj
                              {:invoke-idx inv-idx
                               :ok-idx     (:index op)
                               :partition  (:partition op)
                               :offsets    (:offsets norm)}))

                    :else state))
                {:last-invoke {} :pairs []}
                history))

            ;; Per-partition real-time check
            pairs-by-p (group-by :partition produce-pairs)
            rt-violations
            (into []
              (mapcat
                (fn [[p pairs]]
                  (let [sorted (vec (sort-by :ok-idx pairs))]
                    (for [i (range (count sorted))
                          j (range (inc i) (count sorted))
                          :let [a (nth sorted i)
                                b (nth sorted j)]
                          :when (< (:ok-idx a) (:invoke-idx b))
                          :let [max-a (apply max (:offsets a))
                                min-b (apply min (:offsets b))]
                          :when (>= max-a min-b)]
                      {:partition    p
                       :a-offsets    (:offsets a)
                       :a-ok-idx     (:ok-idx a)
                       :b-offsets    (:offsets b)
                       :b-invoke-idx (:invoke-idx b)})))
                pairs-by-p))

            ;; --- Existing checks: per-partition consumed ordering + completeness ---
            produces-by-p (group-by :partition ok-produces)
            consumes-by-p (group-by :partition ok-consumes)
            partition-results
            (into {}
              (map (fn [[p produces]]
                     (let [;; Flatten (value, offset) pairs for this partition
                           vos      (mapcat (fn [op] (map vector (:values op) (:offsets op)))
                                            produces)
                           expected (->> vos (sort-by second) (mapv first))
                           consumed (mapv :value (get consumes-by-p p []))
                           dups     (- (count consumed) (count (distinct consumed)))]
                       [p {:produced   (count vos)
                           :consumed   (count consumed)
                           :duplicates dups
                           :ordered?   (= consumed
                                         (vec (take (count consumed) expected)))}]))
                   produces-by-p))

            ;; Global completeness
            ;; :ok produces are definitely written; :info are indeterminate
            ;; (may have succeeded). For "lost": only :ok values are expected.
            ;; For "unexpected": accept both :ok and :info values, since an
            ;; indeterminate produce may have written to S3.
            all-produced (set (mapcat :values ok-produces))
            info-produce-values
            (->> history
                 (filter #(and (= :produce (:f %)) (= :info (:type %))))
                 (mapcat #(or (:values %) [(:value %)])))
            all-possibly-produced (into all-produced info-produce-values)
            all-consumed (mapv :value ok-consumes)
            duplicates   (- (count all-consumed) (count (distinct all-consumed)))
            lost         (vec (remove (set all-consumed) all-produced))
            unexpected   (vec (remove all-possibly-produced all-consumed))
            all-ordered? (every? :ordered? (vals partition-results))
            valid?       (and (empty? batch-violations)
                              (empty? rt-violations)
                              (empty? lost)
                              (empty? unexpected)
                              (zero? duplicates)
                              all-ordered?)]
        {:valid?                  valid?
         :batch-violations        (count batch-violations)
         :batch-violation-examples (take 5 batch-violations)
         :rt-violations           (count rt-violations)
         :rt-violation-examples   (take 5 rt-violations)
         :partitions              partition-results
         :produced                (count all-produced)
         :consumed                (count (distinct all-consumed))
         :duplicates              duplicates
         :lost                    (count lost)
         :lost-values             (take 10 lost)
         :unexpected              (count unexpected)
         :in-order?               all-ordered?}))))

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
          (if (:values op)
            ;; Batch produce
            (let [offsets (barka/produce-batch! conn "default" (:partition op) (:values op))]
              (assoc op :type :ok :offsets offsets))
            ;; Single produce
            (let [offset (barka/produce! conn "default" (:partition op) (:value op))]
              (assoc op :type :ok :offset offset)))

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
                                           (let [p (rand-int num-partitions)]
                                             (if (< (rand) 0.5)
                                               ;; Single produce
                                               {:type      :invoke
                                                :f         :produce
                                                :value     (swap! next-value inc)
                                                :partition p}
                                               ;; Batch produce (2-5 values)
                                               (let [batch-size (+ 2 (rand-int 4))
                                                     vals (vec (repeatedly batch-size #(swap! next-value inc)))]
                                                 {:type      :invoke
                                                  :f         :produce
                                                  :values    vals
                                                  :partition p}))))
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
