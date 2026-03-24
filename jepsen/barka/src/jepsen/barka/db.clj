(ns jepsen.barka.db
  "Database (system under test) lifecycle for barka.
   Assumes LocalStack is already running (started via `make localstack`).

   Multi-node: each node gets three processes on unique ports.
   n1 → produce-rpc 9292 / consume-rpc 9293 / jepsen-gateway 9294
   n2 → produce-rpc 9295 / consume-rpc 9296 / jepsen-gateway 9297, etc."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.store :as store])
  (:import (java.lang ProcessBuilder$Redirect)
           (java.net Socket InetSocketAddress HttpURLConnection URL)
           (java.util UUID)))

(def base-produce-rpc-port 9292)
(def base-consume-rpc-port 9293)
(def base-jepsen-gateway-port 9294)

(defn node-idx
  "0-based index from node name, e.g. \"n1\" → 0, \"n2\" → 1."
  [node]
  (dec (parse-long (re-find #"\d+" node))))

(defn produce-rpc-port-for [node]
  (+ base-produce-rpc-port (* 3 (node-idx node))))

(defn consume-rpc-port-for [node]
  (+ base-consume-rpc-port (* 3 (node-idx node))))

(defn jepsen-gateway-port-for [node]
  (+ base-jepsen-gateway-port (* 3 (node-idx node))))

(defn bin-path
  "Resolve a binary name via :bin-dir from opts, or use the bare name (PATH lookup)."
  [opts name]
  (let [dir (get opts :bin-dir nil)]
    (if dir
      (str dir "/" name)
      name)))

(def localstack-port 4566)

(def localstack-endpoint
  (str "http://127.0.0.1:" localstack-port))

(defn tcp-reachable?
  [host port timeout-ms]
  (try
    (let [s (Socket.)]
      (.connect s (InetSocketAddress. ^String host ^int port) ^int timeout-ms)
      (.close s)
      true)
    (catch Exception _ false)))

(defn localstack-s3-ready?
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
  [description pred interval-ms max-attempts]
  (loop [n 0]
    (cond
      (pred)               (info description "ready")
      (>= n max-attempts)  (throw (ex-info (str description " did not become ready") {}))
      :else                (do (Thread/sleep interval-ms)
                               (recur (inc n))))))

(defn- s3-env
  "Common S3/AWS env vars shared by produce-node and consume-node."
  [s3-prefix]
  {"AWS_ENDPOINT_URL"        localstack-endpoint
   "AWS_ACCESS_KEY_ID"       "test"
   "AWS_SECRET_ACCESS_KEY"   "test"
   "AWS_REGION"              "us-east-1"
   "RUST_LOG"                "barka=debug"
   "RUST_BACKTRACE"          "1"
   "BARKA_S3_PREFIX"         s3-prefix})

(defn- start-process
  "Start a process, redirect stdout+stderr to log-file, apply env map."
  [cmd env-map log-file]
  (let [pb (doto (ProcessBuilder. ^java.util.List (vec cmd))
             (.redirectErrorStream true)
             (.redirectOutput (ProcessBuilder$Redirect/appendTo log-file)))]
    (doto (.environment pb)
      (.putAll env-map))
    (.start pb)))

;; --- Per-role start functions (used by setup and nemesis restart) ---

(defn start-produce-node!
  "Starts a produce-node process for the given node. Returns the Process."
  [opts node]
  (let [idx              (node-idx node)
        s3-prefix        (:s3-prefix opts)
        num-partitions   (get opts :num-partitions 4)
        base-env         (s3-env s3-prefix)
        log-dir          (get (:log-dir opts) node)
        log-file         (java.io.File. (str log-dir "/produce-node.log"))
        extra-env        (cond-> {"BARKA_NODE_ID"                   (str idx)
                                  "BARKA_RPC_PORT"                  (str (produce-rpc-port-for node))
                                  "BARKA_TOPICS"                    (str "default:" num-partitions)
                                  "BARKA_LEADER_ELECTION_PREFIX"    s3-prefix
                                  "BARKA_ABDICATION_COOLDOWN_SECS"  "5"
                                  "BARKA_LEADER_ELECTION_POLL_SECS" "1"}
                           (:leader-lease-ttl-ms opts)
                           (assoc "BARKA_LEADER_LEASE_TTL_MS"
                                  (str (:leader-lease-ttl-ms opts)))
                           (:producer-linger-ms opts)
                           (assoc "BARKA_PRODUCER_LINGER_MS"
                                  (str (:producer-linger-ms opts))))]
    (start-process [(bin-path opts "produce-node")]
                   (merge base-env extra-env)
                   log-file)))

(defn start-consume-node!
  "Starts a consume-node process for the given node. Returns the Process."
  [opts node]
  (let [s3-prefix  (:s3-prefix opts)
        base-env   (s3-env s3-prefix)
        log-dir    (get (:log-dir opts) node)
        log-file   (java.io.File. (str log-dir "/consume-node.log"))]
    (start-process [(bin-path opts "consume-node")]
                   (merge base-env
                          {"BARKA_RPC_PORT" (str (consume-rpc-port-for node))})
                   log-file)))

(defn start-gateway!
  "Starts a jepsen-gateway process for the given node. Returns the Process."
  [opts node]
  (let [s3-prefix        (:s3-prefix opts)
        consume-rpc-port (consume-rpc-port-for node)
        gw-port          (jepsen-gateway-port-for node)
        log-dir          (get (:log-dir opts) node)
        log-file         (java.io.File. (str log-dir "/jepsen-gateway.log"))]
    (start-process [(bin-path opts "jepsen-gateway")]
                   {"BARKA_JEPSEN_LISTEN_ADDR"     (str "127.0.0.1:" gw-port)
                    "BARKA_CONSUME_RPC_ADDR"       (str "127.0.0.1:" consume-rpc-port)
                    "BARKA_S3_ENDPOINT"            localstack-endpoint
                    "BARKA_S3_BUCKET"              "barka"
                    "BARKA_AWS_REGION"             "us-east-1"
                    "BARKA_LEADER_ELECTION_PREFIX" s3-prefix
                    "AWS_ACCESS_KEY_ID"            "test"
                    "AWS_SECRET_ACCESS_KEY"        "test"
                    "RUST_LOG"                     "barka=debug"
                    "RUST_BACKTRACE"               "1"}
                   log-file)))

(defn start-rebalancer!
  "Starts the rebalancer binary. Returns the Process."
  [opts log-dir]
  (let [s3-prefix (:s3-prefix opts)
        log-file  (java.io.File. (str log-dir "/rebalancer.log"))]
    (start-process [(bin-path opts "rebalancer")
                    "--interval-secs" "5"
                    "--max-abdications-per-node" "1"
                    "--settle-delay-secs" "2"]
                   {"AWS_ENDPOINT_URL"             localstack-endpoint
                    "AWS_ACCESS_KEY_ID"            "test"
                    "AWS_SECRET_ACCESS_KEY"        "test"
                    "AWS_REGION"                   "us-east-1"
                    "BARKA_S3_BUCKET"              "barka"
                    "BARKA_LEADER_ELECTION_PREFIX" s3-prefix
                    "RUST_LOG"                     "barka=debug"
                    "RUST_BACKTRACE"               "1"}
                   log-file)))

(defn kill-process!
  "Kills a process and waits for it to exit."
  [^Process proc]
  (.destroyForcibly proc)
  (.waitFor proc))

(defn restart-role!
  "Kills and restarts a single role (:produce, :consume, :gateway) on a node.
   Updates the processes atom. Returns the new Process."
  [processes opts node role]
  (when-let [proc (get-in @processes [node role])]
    (info "killing" (name role) "on" node)
    (kill-process! proc))
  (let [start-fn (case role
                   :produce start-produce-node!
                   :consume start-consume-node!
                   :gateway start-gateway!)
        new-proc (start-fn opts node)
        port     (case role
                   :produce (produce-rpc-port-for node)
                   :consume (consume-rpc-port-for node)
                   :gateway (jepsen-gateway-port-for node))]
    (swap! processes assoc-in [node role] new-proc)
    (wait-for (str (name role) " " node)
              #(tcp-reachable? "127.0.0.1" port 100)
              100 50)
    new-proc))

(defn db
  "Constructs a Jepsen db that manages barka.
   Starts three processes per node: produce-node, consume-node, jepsen-gateway.
   Expects :run-id, :s3-prefix, and :num-partitions in opts.
   The `processes` and `log-dirs` atoms are shared with the nemesis for
   kill/restart — they are NOT stored on the test map (Jepsen's Fressian
   serializer cannot handle Process objects)."
  [opts processes log-dirs]
  (let [run-id         (:run-id opts)
        num-partitions (get opts :num-partitions 4)]
    (reify db/DB
      (setup! [_ test node]
        (wait-for "localstack S3" localstack-s3-ready? 200 10)
        (let [produce-rpc-port (produce-rpc-port-for node)
              consume-rpc-port (consume-rpc-port-for node)
              gw-port          (jepsen-gateway-port-for node)
              log-dir          (store/path! test "barka-logs" (str node))
              _                (.mkdirs (java.io.File. (str log-dir)))
              opts             (assoc opts :log-dir {node (str log-dir)})]

          (info "starting barka" node
                "produce-rpc=" produce-rpc-port
                "consume-rpc=" consume-rpc-port
                "jepsen-gateway=" gw-port
                "partitions=" num-partitions)

          (swap! log-dirs assoc node (str log-dir))

          ;; 1. produce-node
          (let [proc (start-produce-node! opts node)]
            (info "produce-node" node "logs →" (str log-dir "/produce-node.log"))
            (swap! processes assoc-in [node :produce] proc))

          ;; 2. consume-node
          (let [proc (start-consume-node! opts node)]
            (info "consume-node" node "logs →" (str log-dir "/consume-node.log"))
            (swap! processes assoc-in [node :consume] proc))

          ;; Wait for both RPC servers before starting gateway
          (wait-for (str "produce-rpc " node)
                    #(tcp-reachable? "127.0.0.1" produce-rpc-port 100)
                    100 50)
          (wait-for (str "consume-rpc " node)
                    #(tcp-reachable? "127.0.0.1" consume-rpc-port 100)
                    100 50)

          ;; 3. jepsen-gateway
          (let [proc (start-gateway! opts node)]
            (info "jepsen-gateway" node "logs →" (str log-dir "/jepsen-gateway.log"))
            (swap! processes assoc-in [node :gateway] proc))

          (wait-for (str "jepsen-gateway " node)
                    #(tcp-reachable? "127.0.0.1" gw-port 100)
                    100 50)
          (info "barka ready on" node)))

      (teardown! [_ test node]
        (info "stopping barka on" node)
        (when-let [node-procs (get @processes node)]
          (doseq [[role proc] node-procs]
            (info "killing" (name role) "on" node)
            (kill-process! proc))
          (swap! processes dissoc node))))))
