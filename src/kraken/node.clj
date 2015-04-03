(ns kraken.node
  (:require [clojure.tools.logging :as log]
            [kraken.gossip :as gossip]))

(defn start
  [options]
  (let [conf {:host (:host options)
              :port (:port options)
              :interval (:interval options)
              :load-state (:load-state options)
              :flush-interval (:flush-interval options)
              :failure-threshold (:failure-threshold options)
              :join false}]
    (log/info "Starting cluster node")
    (gossip/start conf)
    (gossip/join nil [(:host options)])))
