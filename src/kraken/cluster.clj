(ns kraken.cluster
  (:require [clojure.tools.logging :as log]
            [kraken.http :as http]))

(defonce cluster-state (atom {:version 0 :nodes {}}))

(defn normal-nodes
  [nodes]
  (filter :normal @nodes))

(defn down-nodes
  [nodes]
  (filter :down @nodes))

(defn join
  [remote-addr [node]]
  (log/infof "Node joined the cluster %s" node))

(defn leave
  [node cluster-state]
  (log/infof "Node left the cluster %s" node)
  (let [version (:version @cluster-state)
        nodes (:nodes @cluster-state)]
    (swap! cluster-state assoc :version (inc version))
    (swap! cluster-state assoc :nodes (dissoc nodes node))))
