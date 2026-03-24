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

(defn- offsets-monotonic?
  "Returns true if the given sequence of offsets is strictly increasing."
  [offsets]
  (every? (fn [[a b]] (< a b)) (partition 2 1 offsets)))

(defn- offsets-unique?
  "Returns true if no two ops share the same offset."
  [ops]
  (= (count ops) (count (distinct (map :offset ops)))))

(defn log-checker
  "Checks that every produced value is consumed exactly once, in offset order
   within each partition. Handles indeterminate (:info) produces correctly —
   they may or may not have succeeded, so they are excluded from both the
   'lost' and 'unexpected' sets. Also checks offset monotonicity and uniqueness."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ok-produces   (->> history
                               (filter #(and (= :produce (:f %)) (= :ok (:type %)))))
            info-produces (->> history
                               (filter #(and (= :produce (:f %)) (= :info (:type %)))))
            ok-consumes   (->> history
                               (filter #(and (= :consume (:f %)) (= :ok (:type %)))))

            ;; Per-partition analysis
            produces-by-p (group-by :partition ok-produces)
            consumes-by-p (group-by :partition ok-consumes)
            partition-results
            (into {}
              (map (fn [[p produces]]
                     (let [sorted    (->> produces (sort-by :offset))
                           expected  (mapv :value sorted)
                           offsets   (mapv :offset sorted)
                           consumed  (mapv :value (get consumes-by-p p []))
                           dups      (- (count consumed) (count (distinct consumed)))]
                       [p {:produced          (count produces)
                           :consumed          (count consumed)
                           :duplicates        dups
                           :offsets-monotonic? (offsets-monotonic? offsets)
                           :offsets-unique?    (offsets-unique? sorted)
                           :ordered?          (= consumed
                                                (vec (take (count consumed) expected)))}]))
                   produces-by-p))

            ;; Global completeness (with indeterminate handling)
            all-produced    (set (map :value ok-produces))
            maybe-produced  (set (map :value info-produces))
            all-consumed    (mapv :value ok-consumes)
            consumed-set    (set all-consumed)
            duplicates      (- (count all-consumed) (count consumed-set))
            ;; Lost: produced OK but never consumed, excluding indeterminate
            lost            (vec (remove consumed-set all-produced))
            ;; Unexpected: consumed but never produced OK (indeterminate allowed)
            unexpected      (vec (remove (into all-produced maybe-produced) all-consumed))
            all-ordered?    (every? :ordered? (vals partition-results))
            all-monotonic?  (every? :offsets-monotonic? (vals partition-results))
            all-unique?     (every? :offsets-unique? (vals partition-results))
            valid?          (and (empty? lost)
                                 (empty? unexpected)
                                 (zero? duplicates)
                                 all-ordered?
                                 all-monotonic?
                                 all-unique?)]
        {:valid?            valid?
         :partitions        partition-results
         :produced          (count all-produced)
         :indeterminate     (count maybe-produced)
         :consumed          (count consumed-set)
         :duplicates        duplicates
         :lost              (count lost)
         :lost-values       (take 10 lost)
         :unexpected        (count unexpected)
         :unexpected-values (take 10 unexpected)
         :in-order?         all-ordered?
         :offsets-monotonic? all-monotonic?
         :offsets-unique?   all-unique?}))))

(def next-value (atom 0))

(defn barka-client
  "A Jepsen client for barka's distributed log.
   Each partition has its own consume-offset atom.
   Connection is stored in an atom for reconnection after gateway restarts."
  [conn-atom node-ref]
  (reify client/Client
    (open! [this test node]
      (let [a (atom (barka/open "127.0.0.1" (db/jepsen-gateway-port-for node)))]
        (barka-client a (atom node))))

    (setup! [this test])

    (invoke! [this test op]
      (let [c @conn-atom]
        (if (nil? c)
          ;; Try to reconnect
          (try
            (let [new-conn (barka/open "127.0.0.1"
                                       (db/jepsen-gateway-port-for @node-ref))]
              (reset! conn-atom new-conn)
              (assoc op :type :info :error :reconnecting))
            (catch Exception e
              (assoc op :type :info :error (str "reconnect failed: " (.getMessage e)))))
          (try
            (case (:f op)
              :produce
              (let [offset (barka/produce! c "default" (:partition op) (:value op))]
                (assoc op :type :ok :offset offset))

              :consume
              (let [p        (:partition op)
                    off-atom (get (:consume-offsets test) p)
                    off      @off-atom
                    resp     (barka/consume! c "default" p off 1)]
                (cond
                  (empty? (:values resp))
                  (assoc op :type :fail :error :empty)

                  (compare-and-set! off-atom off (:next-offset resp))
                  (assoc op :type :ok :value (parse-long (first (:values resp))))

                  :else
                  (assoc op :type :fail :error :cas-failed)))

              :consume-replay
              (let [p    (:partition op)
                    resp (barka/consume! c "default" p 0 100)]
                (assoc op :type :ok
                       :value (mapv parse-long (:values resp)))))
            (catch java.net.SocketException e
              (reset! conn-atom nil)
              (assoc op :type :info :error (.getMessage e)))
            (catch java.io.IOException e
              (reset! conn-atom nil)
              (assoc op :type :info :error (.getMessage e)))
            (catch Exception e
              (assoc op :type :info :error (.getMessage e)))))))

    (teardown! [this test])

    (close! [this test]
      (when-let [c @conn-atom]
        (barka/close! c)))))

(defn barka-test
  "Constructs a Jepsen test map for barka."
  [opts]
  (reset! next-value 0)
  (let [n               (get opts :num-nodes 3)
        num-partitions  (get opts :num-partitions 4)
        duration        (get opts :duration 30)
        rate            (get opts :rate 50)
        hotspot         (:hotspot-partition opts)
        produce-ratio   (get opts :produce-ratio 0.5)
        drain-duration  (max 15 (quot duration 2))
        node-names      (mapv #(str "n" (inc %)) (range n))
        consume-offsets (into {} (map (fn [p] [p (atom 0)]) (range num-partitions)))
        run-id          (str (UUID/randomUUID))
        s3-prefix       (str "jepsen/" run-id)
        processes       (atom {})
        log-dirs        (atom {})
        opts            (assoc opts :run-id run-id :s3-prefix s3-prefix)
        nemesis-opts    (assoc opts :barka-processes processes)
        pick-partition  (if hotspot
                          #(if (< (rand) 0.7) hotspot (rand-int num-partitions))
                          #(rand-int num-partitions))
        ;; Weight produce vs consume generators
        produce-gen     (fn [_ _]
                          {:type      :invoke
                           :f         :produce
                           :value     (swap! next-value inc)
                           :partition (pick-partition)})
        consume-gen     (fn [_ _]
                          {:type      :invoke
                           :f         :consume
                           :partition (pick-partition)})
        replay-gen      (fn [_ _]
                          {:type      :invoke
                           :f         :consume-replay
                           :partition (rand-int num-partitions)})
        client-gens     (if (== produce-ratio 0.5)
                          [produce-gen consume-gen]
                          ;; Approximate ratio via weighted mix
                          (let [p-weight (int (* 10 produce-ratio))
                                c-weight (- 10 p-weight)]
                            (vec (concat (repeat p-weight produce-gen)
                                         (repeat c-weight consume-gen)))))
        ;; Add replay consumer to the mix
        client-gens     (conj client-gens replay-gen)
        nemesis-gens    [(->> (fn [_ _] {:type :invoke :f :rebalance})
                              (gen/stagger 7))
                         (->> (fn [_ _] {:type :invoke :f :kill-produce})
                              (gen/stagger 12))
                         (->> (fn [_ _] {:type :invoke :f :kill-consume})
                              (gen/stagger 15))
                         (->> (fn [_ _] {:type :invoke :f :kill-gateway})
                              (gen/stagger 15))
                         (->> (fn [_ _] {:type :invoke :f :kill-node})
                              (gen/stagger 25))]]
    (merge tests/noop-test
           opts
           {:name            "barka"
            :concurrency     (* 2 n)
            :consume-offsets consume-offsets
            :num-partitions  num-partitions
            :barka-opts      opts
            :barka-processes processes
            :barka-log-dirs  log-dirs
            :db              (db/db opts processes)
            :client          (barka-client (atom nil) (atom nil))
            :nemesis         (barka-nemesis/combined-nemesis nemesis-opts)
            :checker         (checker/compose
                               {:log      (log-checker)
                                :timeline (timeline/html)
                                :stats    (checker/stats)
                                :perf     (checker/perf)})
            :generator       (gen/phases
                               ;; Phase 1: produce + consume with chaos
                               (gen/any
                                 (->> (gen/mix client-gens)
                                      (gen/stagger (/ 1 rate))
                                      (gen/clients)
                                      (gen/time-limit duration))
                                 (->> (gen/mix nemesis-gens)
                                      (gen/nemesis)
                                      (gen/time-limit duration)))
                               ;; Phase 2: drain all partitions
                               (gen/log "Draining...")
                               (->> (fn [_ _]
                                      {:type      :invoke
                                       :f         :consume
                                       :partition (rand-int num-partitions)})
                                    (gen/stagger 1/100)
                                    (gen/clients)
                                    (gen/time-limit drain-duration)))
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
    :validate [#(<= 1 % 64) "Must be between 1 and 64"]]
   [nil "--duration SECS" "Test duration in seconds"
    :default 30
    :parse-fn parse-long
    :validate [#(pos? %) "Must be positive"]]
   [nil "--rate NUM" "Target ops/sec"
    :default 50
    :parse-fn parse-long
    :validate [#(pos? %) "Must be positive"]]
   [nil "--hotspot-partition NUM" "Focus 70% of traffic on this partition"
    :parse-fn parse-long]
   [nil "--produce-ratio FLOAT" "Fraction of ops that are produces (0.0-1.0)"
    :default 0.5
    :parse-fn #(Double/parseDouble %)
    :validate [#(< 0.0 % 1.0) "Must be between 0 and 1"]]
   [nil "--leader-lease-ttl-ms MS" "Leader lease TTL in milliseconds (requires Rust-side support)"
    :parse-fn parse-long
    :validate [#(>= % 1000) "Must be >= 1000ms"]]
   [nil "--producer-linger-ms MS" "Producer batch linger timeout in milliseconds"
    :parse-fn parse-long
    :validate [#(pos? %) "Must be positive"]]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run!
    (cli/single-test-cmd {:test-fn  barka-test
                           :opt-spec cli-opts})
    args))
