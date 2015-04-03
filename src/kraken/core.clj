(ns kraken.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [kraken.node :as node])
  (:import (java.net InetAddress))
  (:gen-class))

(def cli-options
  [["-H" "--host HOST" "Service hostname"
    :default (.getHostAddress (InetAddress/getByName "localhost"))
    :parse-fn #(.getHostAddress (InetAddress/getByName %))]
   ["-p" "--port PORT" "Port number"
    :default 10001
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-l" "--load-state BOOL" "Load cluster state from disk"
    :default true
    :parse-fn #(Boolean/parseBoolean %)]
   ["-i" "--interval INTERVAL" "Heartbeat interval (sec)"
    :default 1000
    :parse-fn #(Integer/parseInt %)]
   ["-f" "--flush-interval" "Flush cluster state interval"
    :default 5000
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 1 % 0x10000) "Must be a number between 1 and 65536"]]
   ["-t" "--failure-threshold" "Failure detection threshold"
    :default 8
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 2 % 0x14) "Must be a number between 2 and 20 (Default is 8)"]]])

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]}
        (parse-opts args cli-options)]
    (log/infof "Kraken v%s - Cluster Server" "0.1")
    (node/start options)))
