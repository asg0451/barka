(ns jepsen.barka.db
  "Database (system under test) lifecycle for barka.
   Assumes LocalStack is already running (started via `make localstack`).

   Multi-node: each node gets its own barka process on a unique port pair.
   n1 → rpc 9292 / jepsen gateway 9293, n2 → rpc 9294 / jepsen gateway 9295, etc."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.store :as store])
  (:import (java.lang ProcessBuilder$Redirect)
           (java.net Socket InetSocketAddress HttpURLConnection URL)))

(def barka-bin
  "Path to the barka binary. Override via :barka-bin in test opts."
  "barka")

(def base-rpc-port 9292)
(def base-jepsen-gateway-port 9293)

(defn node-idx
  "0-based index from node name, e.g. \"n1\" → 0, \"n2\" → 1."
  [node]
  (dec (parse-long (re-find #"\d+" node))))

(defn rpc-port-for [node]
  (+ base-rpc-port (* 2 (node-idx node))))

(defn jepsen-gateway-port-for [node]
  (+ base-jepsen-gateway-port (* 2 (node-idx node))))

(def localstack-port
  "LocalStack gateway port."
  4566)

(def localstack-endpoint
  "S3-compatible endpoint URL for LocalStack."
  (str "http://127.0.0.1:" localstack-port))

(defn tcp-reachable?
  "Returns true if a TCP connection to host:port succeeds within timeout-ms."
  [host port timeout-ms]
  (try
    (let [s (Socket.)]
      (.connect s (InetSocketAddress. ^String host ^int port) ^int timeout-ms)
      (.close s)
      true)
    (catch Exception _ false)))

(defn localstack-s3-ready?
  "Returns true if LocalStack's S3 is responding to health checks."
  []
  (try
    (let [url  (URL. (str localstack-endpoint "/_localstack/health"))
          conn (doto ^HttpURLConnection (.openConnection url)
                 (.setConnectTimeout 500)
                 (.setReadTimeout 500)
                 (.setRequestMethod "GET"))]
      (= 200 (.getResponseCode conn)))
    (catch Exception _ false)))

(defn wait-for
  "Polls pred every interval-ms up to max-attempts times. Throws if never satisfied."
  [description pred interval-ms max-attempts]
  (loop [n 0]
    (cond
      (pred)               (info description "ready")
      (>= n max-attempts)  (throw (ex-info (str description " did not become ready") {}))
      :else                (do (Thread/sleep interval-ms)
                               (recur (inc n))))))

(defn db
  "Constructs a Jepsen db that manages barka.
   Assumes LocalStack is already running (`make localstack`).
   Starts one barka process per node, each on its own port pair."
  [opts]
  (let [bin       (get opts :barka-bin barka-bin)
        processes (atom {})]
    (reify db/DB
      (setup! [_ test node]
        (wait-for "localstack S3" localstack-s3-ready? 200 10)
        (let [rpc-port (rpc-port-for node)
              gw-port  (jepsen-gateway-port-for node)
              idx      (node-idx node)]
          (info "starting barka" node "rpc=" rpc-port "jepsen-gateway=" gw-port)
          (let [log-file (store/path! test "barka-logs" (str node) "barka.log")
                pb       (doto (ProcessBuilder. [bin])
                           (.redirectErrorStream true)
                           (.redirectOutput (ProcessBuilder$Redirect/appendTo log-file)))]
            (info "barka" node "logs →" (.getAbsolutePath log-file))
            (doto (.environment pb)
              (.put "AWS_ENDPOINT_URL" localstack-endpoint)
              (.put "AWS_ACCESS_KEY_ID" "test")
              (.put "AWS_SECRET_ACCESS_KEY" "test")
              (.put "AWS_REGION" "us-east-1")
              (.put "RUST_LOG" "debug")
              (.put "RUST_BACKTRACE" "1")
              (.put "BARKA_NODE_ID" (str idx))
              (.put "BARKA_RPC_PORT" (str rpc-port))
              (.put "BARKA_JEPSEN_GATEWAY_PORT" (str gw-port)))
            (let [proc (.start pb)]
              (swap! processes assoc node proc)
              (wait-for (str "barka jepsen gateway " node)
                        #(tcp-reachable? "127.0.0.1" gw-port 100)
                        100 50)
              (info "barka ready on" node)))))

      (teardown! [_ test node]
        (info "stopping barka on" node)
        (when-let [proc (get @processes node)]
          (.destroyForcibly proc)
          (.waitFor proc)
          (swap! processes dissoc node))))))
