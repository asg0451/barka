(ns jepsen.barka.nemesis
  "Chaos nemeses for barka:
   - rebalancer-nemesis: forces leadership shuffles via abdicate RPCs
   - process-nemesis: kills and restarts individual processes or whole nodes"
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

(defn- build-restart-opts
  "Builds the opts map needed by db/restart-role! from the test map."
  [test node]
  (let [base-opts (:barka-opts test)
        log-dir   (get @(:barka-log-dirs test) node)]
    (assoc base-opts :log-dir {node log-dir})))

(defn process-nemesis
  "Creates a Jepsen nemesis that kills and restarts barka processes.
   Handles :kill-produce, :kill-consume, :kill-gateway (single role on random
   node) and :kill-node (all three roles on a random node)."
  [opts]
  (let [processes (:barka-processes opts)]
    (reify nemesis/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (try
          (let [nodes (:nodes test)
                node  (rand-nth nodes)
                do-kill-restart
                (fn [role]
                  (let [ropts (build-restart-opts test node)]
                    (info "nemesis: killing" (name role) "on" node)
                    (when-let [proc (get-in @processes [node role])]
                      (db/kill-process! proc)
                      (swap! processes update node dissoc role))
                    ;; Brief pause before restart
                    (Thread/sleep (+ 1000 (rand-int 2000)))
                    (info "nemesis: restarting" (name role) "on" node)
                    (db/restart-role! processes ropts node role)))]
            (case (:f op)
              :kill-produce
              (do (do-kill-restart :produce)
                  (assoc op :type :info :value {:node node :killed :produce}))

              :kill-consume
              (do (do-kill-restart :consume)
                  (assoc op :type :info :value {:node node :killed :consume}))

              :kill-gateway
              (do (do-kill-restart :gateway)
                  (assoc op :type :info :value {:node node :killed :gateway}))

              :kill-node
              (do (doseq [role [:gateway :produce :consume]]
                    (when-let [proc (get-in @processes [node role])]
                      (info "nemesis: killing" (name role) "on" node)
                      (db/kill-process! proc)
                      (swap! processes update node dissoc role)))
                  (Thread/sleep (+ 1000 (rand-int 2000)))
                  ;; Restart in dependency order: produce, consume, then gateway
                  (let [ropts (build-restart-opts test node)]
                    (doseq [role [:produce :consume :gateway]]
                      (info "nemesis: restarting" (name role) "on" node)
                      (db/restart-role! processes ropts node role)))
                  (assoc op :type :info :value {:node node :killed :all}))))
          (catch Exception e
            (warn "process nemesis failed:" (.getMessage e))
            (assoc op :type :info :value :error))))

      (teardown! [this test]))))

(defn combined-nemesis
  "Composes the rebalancer nemesis with the process-killer nemesis."
  [opts]
  (nemesis/compose
    {#{:rebalance}                          (rebalancer-nemesis opts)
     #{:kill-produce :kill-consume
       :kill-gateway :kill-node}            (process-nemesis opts)}))
