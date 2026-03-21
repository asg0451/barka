(ns jepsen.barka.db
  "Database (system under test) lifecycle for barka.
   Starts and stops barka as a local process."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]))

(def barka-bin
  "Path to the barka binary. Override via :barka-bin in test opts."
  "barka")

(def control-port
  "Default control API port."
  9293)

(def rpc-port
  "Default capnp-rpc port."
  9292)

(defn db
  "Constructs a Jepsen db that manages a local barka process."
  [opts]
  (let [bin     (get opts :barka-bin barka-bin)
        process (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        (info "starting barka on" node)
        (let [proc (.start (ProcessBuilder. [bin]))]
          (reset! process proc)
          ;; Wait for control port to be ready
          (loop [attempts 0]
            (if (>= attempts 50)
              (throw (ex-info "barka did not start in time" {:node node}))
              (if (try
                    (let [s (java.net.Socket.)]
                      (.connect s (java.net.InetSocketAddress. "127.0.0.1" control-port) 100)
                      (.close s)
                      true)
                    (catch Exception _
                      false))
                (info "barka ready on" node)
                (do (Thread/sleep 100)
                    (recur (inc attempts))))))))

      (teardown! [_ test node]
        (info "stopping barka on" node)
        (when-let [proc @process]
          (.destroyForcibly proc)
          (.waitFor proc)
          (reset! process nil))))))
