(ns jepsen.barka.core
  "Jepsen test for barka distributed log.

   Workload: multi-partition log correctness with rebalancer chaos.
   - :produce appends values to a random partition (single or batch)
   - :consume reads the next unconsumed value from a partition (per-partition offset)
   - Checker validates:
     1. Intra-batch ordering: offsets within a batch are strictly increasing
     2. Real-time ordering: if produce A completes before B is invoked (same
        partition), max(A.offsets) < min(B.offsets)
     3. Offset monotonicity and uniqueness per partition
     4. Consumed-offset ordering and global completeness (no lost/dupes)"
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

(defn- offsets-unique?
  "Returns true if no two offsets in the seq are the same."
  [offsets]
  (= (count offsets) (count (distinct offsets))))

(defn log-checker
  "Checks ordering invariants and completeness for the distributed log.

   Invariants per partition:
   1. Intra-batch: each batch produce's offsets are strictly increasing
   2. Real-time: if produce A :ok precedes produce B :invoke in history,
      max(A.offsets) < min(B.offsets)
   3. Offset monotonicity and uniqueness across all produces per partition
   4. Consumed values appear in server-assigned offset order
   5. Completeness: no lost values, no unexpected values, no duplicates

   Handles indeterminate (:info) produces correctly — they may or may not
   have succeeded, so they are excluded from both 'lost' and 'unexpected'."
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
            info-produces (->> history
                               (filter #(and (= :produce (:f %)) (= :info (:type %)))))
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

            ;; --- Invariant 3: Per-partition offset uniqueness ---
            ;; No two produces share the same offset (catches split-brain).
            ;; Note: chronological monotonicity (later invocation → higher offset)
            ;; is NOT checked here because concurrent producers can legitimately
            ;; receive offsets out of invocation order. The real-time ordering
            ;; check (invariant 2) handles the correct linearizable constraint.
            produces-by-p (group-by :partition ok-produces)
            consumes-by-p (group-by :partition ok-consumes)

            all-offsets-by-p
            (into {}
              (map (fn [[p produces]]
                     [p (->> produces
                             (mapcat :offsets)
                             vec)])
                   produces-by-p))

            offset-unique-by-p
            (into {} (map (fn [[p offs]] [p (offsets-unique? offs)])
                          all-offsets-by-p))

            ;; --- Invariant 4: Consumed ordering + completeness ---
            partition-results
            (into {}
              (map (fn [[p produces]]
                     (let [vos      (mapcat (fn [op] (map vector (:values op) (:offsets op)))
                                            produces)
                           expected (->> vos (sort-by second) (mapv first))
                           consumed (mapv :value (get consumes-by-p p []))
                           dups     (- (count consumed) (count (distinct consumed)))]
                       [p {:produced          (count vos)
                           :consumed          (count consumed)
                           :duplicates        dups
                           :offsets-unique?    (get offset-unique-by-p p true)
                           :ordered?          (= consumed
                                                (vec (take (count consumed) expected)))}]))
                   produces-by-p))

            ;; --- Invariant 5: Global completeness (with indeterminate handling) ---
            all-produced (set (mapcat :values ok-produces))
            info-produce-values
            (->> info-produces
                 (mapcat #(or (:values %) [(:value %)])))
            maybe-produced (set info-produce-values)
            all-possibly-produced (into all-produced maybe-produced)
            all-consumed    (mapv :value ok-consumes)
            consumed-set    (set all-consumed)
            duplicates      (- (count all-consumed) (count consumed-set))
            lost            (vec (remove consumed-set all-produced))
            unexpected      (vec (remove all-possibly-produced all-consumed))

            ;; --- Invariant 6: Replay consume validation ---
            ;; Each :consume-replay reads from offset 0. We check that the
            ;; values returned contain no unexpected values (values that were
            ;; never produced as :ok or :info). We can't do a strict prefix
            ;; match because :info produces may appear in S3 but not in
            ;; expected-by-p (built from :ok only).
            ok-replays  (->> history
                             (filter #(and (= :consume-replay (:f %))
                                          (= :ok (:type %)))))
            replay-violations
            (->> ok-replays
                 (remove (fn [op]
                           (let [got (set (:value op))]
                             (every? all-possibly-produced got))))
                 (mapv (fn [op]
                         {:partition (:partition op)
                          :got       (take 5 (:value op))
                          :bad       (take 5 (remove all-possibly-produced (:value op)))})))

            all-ordered?    (every? :ordered? (vals partition-results))
            all-unique?     (every? :offsets-unique? (vals partition-results))
            valid?          (and (empty? batch-violations)
                                 (empty? rt-violations)
                                 (empty? replay-violations)
                                 (empty? lost)
                                 (empty? unexpected)
                                 (zero? duplicates)
                                 all-ordered?
                                 all-unique?)]
        {:valid?                  valid?
         :batch-violations        (count batch-violations)
         :batch-violation-examples (take 5 batch-violations)
         :rt-violations           (count rt-violations)
         :rt-violation-examples   (take 5 rt-violations)
         :replay-violations       (count replay-violations)
         :replay-violation-examples (take 5 replay-violations)
         :partitions              partition-results
         :produced                (count all-produced)
         :indeterminate           (count maybe-produced)
         :consumed                (count consumed-set)
         :duplicates              duplicates
         :lost                    (count lost)
         :lost-values             (take 10 lost)
         :unexpected              (count unexpected)
         :unexpected-values       (take 10 unexpected)
         :in-order?               all-ordered?
         :offsets-unique?         all-unique?}))))

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
              (if (:values op)
                ;; Batch produce
                (let [offsets (barka/produce-batch! c "default" (:partition op) (:values op))]
                  (assoc op :type :ok :offsets offsets))
                ;; Single produce
                (let [offset (barka/produce! c "default" (:partition op) (:value op))]
                  (assoc op :type :ok :offset offset)))

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
        _               (when (and hotspot (>= hotspot num-partitions))
                          (throw (ex-info "hotspot-partition must be < num-partitions"
                                          {:hotspot hotspot :num-partitions num-partitions})))
        produce-ratio   (get opts :produce-ratio 0.5)
        drain-duration  (max 15 (quot duration 2))
        node-names      (mapv #(str "n" (inc %)) (range n))
        consume-offsets (into {} (map (fn [p] [p (atom 0)]) (range num-partitions)))
        run-id          (str (UUID/randomUUID))
        s3-prefix       (str "jepsen/" run-id)
        processes       (atom {})
        log-dirs        (atom {})
        opts            (assoc opts :run-id run-id :s3-prefix s3-prefix)
        nemesis-opts    (assoc opts
                               :barka-processes processes
                               :barka-log-dirs  log-dirs
                               :barka-base-opts opts)
        pick-partition  (if hotspot
                          #(if (< (rand) 0.7) hotspot (rand-int num-partitions))
                          #(rand-int num-partitions))
        ;; Produce generator: 50/50 single vs batch
        produce-gen     (fn [_ _]
                          (let [p (pick-partition)]
                            (if (< (rand) 0.5)
                              {:type      :invoke
                               :f         :produce
                               :value     (swap! next-value inc)
                               :partition p}
                              (let [batch-size (+ 2 (rand-int 4))
                                    vals (vec (repeatedly batch-size #(swap! next-value inc)))]
                                {:type      :invoke
                                 :f         :produce
                                 :values    vals
                                 :partition p}))))
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
                          (let [p-weight (int (* 10 produce-ratio))
                                c-weight (- 10 p-weight)]
                            (vec (concat (repeat p-weight produce-gen)
                                         (repeat c-weight consume-gen)))))
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
            :db              (db/db opts processes log-dirs)
            :client          (barka-client (atom nil) (atom nil))
            :nemesis         (barka-nemesis/combined-nemesis nemesis-opts)
            :checker         (checker/compose
                               {:log      (log-checker)
                                :timeline (timeline/html)
                                :stats    (checker/stats)})
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
