(ns kraken.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [kraken.server :as server])
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
   ["-i" "--interval INTERVAL" "Heartbeat interval (sec)"
    :default 1000
    :parse-fn #(Integer/parseInt %)]])

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]}
        (parse-opts args cli-options)]
    (log/infof "Kraken v%s - Cluster Server" "1.0")
    (server/start options)))
