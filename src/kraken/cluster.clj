(ns kraken.cluster
  (:require [clojure.tools.logging :as log]
            [kraken.http :as http]
            [kraken.gossip :as gossip]))

(def nodes (atom {}))

(defn active-nodes
  []
  (filter :active @nodes))

(defn down-nodes
  []
  (filter :down @nodes))

(defn join
  [remote-addr [node]]
  (if (contains? @nodes node)
    {:status 200
     :body :member_exists}
    (let [ping (gossip/ping node)]
      (if (not= ping :pong)
        (do
          (log/infof "Node is unreachable %s" node)
          {:status 400
           :body :node_unreachable})
        (do
          (log/infof "Node joined the cluster %s" node)
          (swap! nodes assoc node :active)
          (future (gossip/distribute-state nodes))
          {:status 201
           :body {:nodes @nodes}})))))

(defn leave
  [remote-addr [node]]
  (log/infof "Node left the cluster %s" node)
  (swap! nodes dissoc node)
  (future (gossip/distribute-state @nodes))
  {:status 200
   :body :ok})

(defn state
  [remote-addr [node-state]]
  (log/infof "Received cluster state %s from %s" node-state remote-addr)
  (if (not= node-state nodes)
    (swap! nodes node-state)))

(defn elect
  [args]
  (log/info "elect a master")
  {:status 200
   :body :elect})

(defn ping
  []
  (log/info "Ping")
  {:status 200
   :body :pong})
