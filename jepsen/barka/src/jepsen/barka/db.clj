(ns jepsen.barka.db
  "Database (system under test) lifecycle for barka.
   Starts LocalStack (S3) via docker-compose, then barka as a local process."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
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

(defn start-localstack!
  "Starts LocalStack via docker-compose from the project root."
  [project-root]
  (info "starting localstack via docker compose")
  (let [pb (doto (ProcessBuilder. ["docker" "compose" "up" "-d" "--wait"])
             (.directory (java.io.File. ^String project-root))
             (.inheritIO))]
    (let [proc (.start pb)
          exit (.waitFor proc)]
      (when-not (zero? exit)
        (throw (ex-info "docker compose up failed" {:exit exit})))))
  (wait-for "localstack S3" localstack-s3-ready? 200 50))

(defn stop-localstack!
  "Stops LocalStack via docker-compose."
  [project-root]
  (info "stopping localstack")
  (let [pb (doto (ProcessBuilder. ["docker" "compose" "down" "-v"])
             (.directory (java.io.File. ^String project-root))
             (.inheritIO))]
    (let [proc (.start pb)]
      (.waitFor proc))))

(defn db
  "Constructs a Jepsen db that manages LocalStack + barka."
  [opts]
  (let [bin          (get opts :barka-bin barka-bin)
        project-root (get opts :project-root
                          (-> (java.io.File. ".") .getCanonicalFile .getParent
                              (java.io.File.) .getParent str))
        process      (atom nil)]
    (reify db/DB
      (setup! [_ test node]
        (start-localstack! project-root)
        (info "starting barka on" node "with S3 endpoint" localstack-endpoint)
        (let [env (into-array String
                    [(str "AWS_ENDPOINT_URL=" localstack-endpoint)
                     (str "AWS_ACCESS_KEY_ID=test")
                     (str "AWS_SECRET_ACCESS_KEY=test")
                     (str "AWS_REGION=us-east-1")
                     (str "RUST_LOG=info")])
              pb  (doto (ProcessBuilder. [bin])
                    (.environment))]
          ;; ProcessBuilder doesn't have a bulk setenv; use the env map.
          (let [pb-env (.environment pb)]
            (doseq [pair env]
              (let [[k v] (str/split pair #"=" 2)]
                (.put pb-env k v))))
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
          (reset! process nil))
        (stop-localstack! project-root)))))
