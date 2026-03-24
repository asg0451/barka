(ns jepsen.barka.nemesis
  "Chaos nemesis that forces partition leadership shuffles by sending abdicate
   commands through the jepsen-gateway. Uses the same abdicate RPC mechanism
   that the rebalancer uses internally."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.nemesis :as nemesis]
            [jepsen.barka.client :as barka]
            [jepsen.barka.db :as db]))

(defn- abdicate-partition!
  "Sends an abdicate request through a gateway connection.
   Returns true if the gateway accepted it, false on error."
  [conn topic partition]
  (try
    (let [resp (barka/request! conn {:op "abdicate"
                                     :topic topic
                                     :partition partition})]
      (:ok resp))
    (catch Exception e
      (warn "abdicate failed:" (.getMessage e))
      false)))

(defn rebalancer-nemesis
  "Creates a Jepsen nemesis that forces leadership changes by abdicating
   random partitions through the gateway. On each :rebalance invocation,
   picks a random node's gateway and abdicates all partitions through it."
  [opts]
  (let [num-partitions (get opts :num-partitions 4)]
    (reify nemesis/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (case (:f op)
          :rebalance
          (try
            (let [nodes  (:nodes test)
                  node   (rand-nth nodes)
                  port   (db/jepsen-gateway-port-for node)
                  conn   (barka/open "127.0.0.1" port)
                  parts  (shuffle (range num-partitions))
                  ;; Abdicate a random subset (1 to all partitions)
                  n-abd  (inc (rand-int num-partitions))
                  target (take n-abd parts)
                  results (doall
                            (for [p target]
                              (do
                                (info "abdicating partition" p "via" node)
                                (abdicate-partition! conn "default" p))))]
              (barka/close! conn)
              (let [succeeded (count (filter true? results))]
                (assoc op :type :info
                       :value {:node node
                               :abdicated succeeded
                               :attempted (count results)})))
            (catch Exception e
              (warn "nemesis invocation failed:" (.getMessage e))
              (assoc op :type :info :value :error)))))

      (teardown! [this test]))))
