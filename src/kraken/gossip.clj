(ns kraken.gossip
  (:require [clojure.tools.logging :as log]
            [ring.adapter.jetty :as jetty]
            [kraken.cluster :as cluster]
            [kraken.middleware :as middleware]
            [kraken.http :as http]))

(defonce cluster-state (atom {:version 0 :nodes {}}))

(defn ping
  [[node-state]]
  (log/info "Ping")
  (if (> (:version node-state) (:version @cluster-state))
    (reset! cluster-state node-state))
  {:status 200
   :body :pong})

(defn ping-node
  ""
  [node]
  (let [url (str "http://" node ":10001")
        body [:ping @cluster-state]
        response (try
                   (http/post url body)
                   (catch Exception e
                     nil))]
    (:body response)))

;(defn send-to-node)

(defn inc-version
  []
  (let [version (:version @cluster-state)]
    (swap! cluster-state assoc :version (inc version))))

(defn distribute-state
  [nodes]
  (log/infof "Distribute state %s" @cluster-state)
  (pmap (fn [[node args]]
          (let [url (str "http://" node ":10001")]
            (http/post url [:state @cluster-state])))
        (:nodes @cluster-state)))

(defn join
  [remote-addr [node]]
  (if (contains? (:nodes @cluster-state) node)
    (do
      (log/infof "Node already a member %s" node)
      {:status 200
       :body :member_exists})
    (let [ping (ping-node node)]
      (if (not= ping :pong)
        (do
          (log/infof "Node is unreachable %s" node)
          {:status 400
           :body :node_unreachable})
        (do
          (log/infof "Node joined the cluster %s" node)
          (inc-version)
          (swap! cluster-state assoc-in [:nodes node] :active)
          ;(future (distribute-state nodes))
          {:status 201
           :body {:state @cluster-state}})))))

(defn leave
  [remote-addr [node]]
  (if (contains? (:nodes @cluster-state) node)
    (do
      (log/infof "Node leaving the cluster %s" node)
      (inc-version)
      (swap! cluster-state assoc-in [:nodes node] :leaving)
      (future (cluster/leave node cluster-state))
      ;(future (distribute-state nodes))
      {:status 200
       :body :ok})
    (do
      (log/infof "Node not found %s" node)
      {:status 200
       :body :not_a_member})))

(defn elect
  [args]
  (log/info "elect a master")
  {:status 200
   :body :elect})

(defn state
  [remote-addr [node-state]]
  (log/infof "Received cluster state %s from %s" node-state remote-addr)
  (if (not= node-state @cluster-state)
    (reset! cluster-state node-state)))

(defn get-random-node
  [me]
  (let [others (filter (fn [node-info]
                         (not= (first node-info) me))
                       (:nodes @cluster-state))]
    (if (> (count others) 0)
      (rand-nth others))))

(defn heartbeat
  [me interval]
  (log/infof "Starting heartbeat")
  (future
    (try
      (loop [node (get-random-node me)]
        (if (nil? node)
          (do
            (log/infof "No other nodes in the cluster...%s" (:nodes @cluster-state))
            (Thread/sleep interval)
            (recur (get-random-node me)))
          (do
            (log/infof "ping node=%s, state=%s" node @cluster-state)
            (ping-node (first node))
            (Thread/sleep interval)
            (recur (get-random-node me)))))
      (catch Exception e
        (log/error e "blah")))))

(defn handler
  [request]
  (let [body (:body request)
        remote-addr (:remote-addr request)
        command (first body)
        args (rest body)]
    (condp = command
      :join (join remote-addr args)
      :leave (leave remote-addr args)
      :state (state remote-addr args)
      :elect (elect args)
      :ping (ping args)
      (ping))))

(def app
  (-> handler
      middleware/wrap-edn-body
      middleware/wrap-no-routes
      (middleware/wrap-content-type "application/edn")))

(defn start
  [conf]
  (let [host (:host conf)
        port (:port conf)
        interval (:interval conf)]
    (log/infof "Starting gossip listener at %s:%s" host port)
    (heartbeat host interval)
    (future (jetty/run-jetty app conf))))
