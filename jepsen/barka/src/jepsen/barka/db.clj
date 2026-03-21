(ns jepsen.barka.db
  "Database (system under test) lifecycle for barka.
   Assumes LocalStack is already running (started via `make localstack`)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db])
  (:import (java.net Socket InetSocketAddress HttpURLConnection URL)))

(def barka-bin
  "Path to the barka binary. Override via :barka-bin in test opts."
  "barka")

(def control-port
  "Default control API port."
  9293)

(def rpc-port
  "Default capnp-rpc port."
  9292)

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
   Assumes LocalStack is already running (`make localstack`)."
  [opts]
  (let [bin     (get opts :barka-bin barka-bin)
        process (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        (wait-for "localstack S3" localstack-s3-ready? 200 10)
        (info "starting barka on" node "with S3 endpoint" localstack-endpoint)
        (let [pb (ProcessBuilder. [bin])]
          (doto (.environment pb)
            (.put "AWS_ENDPOINT_URL" localstack-endpoint)
            (.put "AWS_ACCESS_KEY_ID" "test")
            (.put "AWS_SECRET_ACCESS_KEY" "test")
            (.put "AWS_REGION" "us-east-1")
            (.put "RUST_LOG" "info"))
          (let [proc (.start pb)]
            (reset! process proc)
            (wait-for "barka control port"
                      #(tcp-reachable? "127.0.0.1" control-port 100)
                      100 50)
            (info "barka ready on" node))))

      (teardown! [_ test node]
        (info "stopping barka on" node)
        (when-let [proc @process]
          (.destroyForcibly proc)
          (.waitFor proc)
          (reset! process nil))))))
