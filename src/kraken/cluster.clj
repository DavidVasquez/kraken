(ns kraken.cluster
  (:require [clojure.tools.logging :as log]
            [kraken.http :as http]))

(defn active-nodes
  [nodes]
  (filter :active @nodes))

(defn down-nodes
  [nodes]
  (filter :down @nodes))

(defn join
  [remote-addr [node]]
  (log/infof "Node joined the cluster %s" node))

(defn leave
  [node nodes]
  (log/infof "Node left the cluster %s" node)
  (swap! nodes dissoc node))
