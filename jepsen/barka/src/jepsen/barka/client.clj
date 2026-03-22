(ns jepsen.barka.client
  "TCP/JSON client for barka's Jepsen API gateway.

   Protocol: newline-delimited JSON over TCP.
   Send: {\"op\":\"produce\",\"partition\":0,\"value\":\"hello\"}
   Recv: {\"ok\":true,\"offset\":0}"
  (:require [cheshire.core :as json]
            [clojure.tools.logging :refer [info warn]])
  (:import (java.io BufferedReader InputStreamReader PrintWriter)
           (java.net Socket)))

(def ^:private default-so-timeout-ms
  "Read timeout for each request/response round-trip (server uses 10s RPC cap)."
  15000)

(defn open
  "Opens a TCP connection to a barka node's Jepsen gateway port.
   Returns a map with :socket, :in (BufferedReader), :out (PrintWriter).
   Socket read timeout: `BARKA_JEPSEN_CLIENT_TIMEOUT_MS` env or 15000 ms."
  [host port]
  (let [sock (Socket. ^String host ^int port)
        timeout-ms (or (some-> (System/getenv "BARKA_JEPSEN_CLIENT_TIMEOUT_MS")
                               Long/parseLong)
                      default-so-timeout-ms)]
    (.setSoTimeout sock (int timeout-ms))
    (let [in   (BufferedReader. (InputStreamReader. (.getInputStream sock)))
          out  (PrintWriter. (.getOutputStream sock) true)]
      {:socket sock :in in :out out})))

(defn close!
  "Closes a client connection."
  [{:keys [^Socket socket]}]
  (.close socket))

(defn request!
  "Sends a JSON request and reads a JSON response. Returns parsed map."
  [{:keys [^BufferedReader in ^PrintWriter out]} req]
  (.println out (json/generate-string req))
  (let [line (.readLine in)]
    (when (nil? line)
      (throw (ex-info "connection closed" {:req req})))
    (json/parse-string line true)))

(defn produce!
  "Sends a produce request. Returns the offset."
  [conn topic partition value]
  (let [resp (request! conn {:op "produce"
                             :topic topic
                             :partition partition
                             :value (str value)})]
    (if (:ok resp)
      (:offset resp)
      (throw (ex-info "produce failed" resp)))))

(defn consume!
  "Sends a consume request. Returns vector of string values."
  [conn topic partition offset max-records]
  (let [resp (request! conn {:op "consume"
                             :topic topic
                             :partition partition
                             :offset offset
                             :max max-records})]
    (if (:ok resp)
      (:values resp)
      (throw (ex-info "consume failed" resp)))))
