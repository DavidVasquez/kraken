(ns kraken.gossip
  (:import [java.net ConnectException SocketException])
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :refer [as-file]]
            [clojure.core.async :as async :refer [<! timeout chan go-loop]]
            [ring.adapter.jetty :as jetty]
            [kraken.cluster :as cluster]
            [kraken.middleware :as middleware]
            [kraken.http :as http]))

(defonce cluster-state (atom {:version 0 :nodes {}}))

(defonce failure-threshold (atom 0))

(defonce ping-failures (atom {}))

(defn inc-node-failure
  [node]
  (let [new-count (inc (get @ping-failures node 0))]
    (log/info "fcount" new-count "node=" node)
    (swap! ping-failures assoc node new-count)))

(defn reset-node-failure
  [node]
  (swap! ping-failures assoc node 0))

(defn inc-version
  []
  (let [version (:version @cluster-state)]
    (swap! cluster-state assoc :version (inc version))))

(defn down
  [node]
  (log/errorf "Node appears to be down %s" node)
  (dosync
    (inc-version)
    (swap! cluster-state assoc-in [:nodes node] :down)))

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

(defn ping
  [remote-addr [node-state]]
  (log/tracef "ping from=%s" remote-addr)
  (if (> (:version node-state) (:version @cluster-state))
    (reset! cluster-state node-state))
  {:status 200
   :body :pong})

(defn ping-node
  ""
  [node]
  (log/debugf "ping to=%s, state=%s" node @cluster-state)
  (let [url (str "http://" node ":10001")
        body [:ping @cluster-state]
        response (try
                   (http/post url body)
                   (catch SocketException e
                     (log/infof "ping failure to=%s" node))
                   (catch ConnectException e
                     (log/infof "ping failure to=%s" node))
                   (catch Exception e
                     (log/error e "ping-node error")))]
    (:body response)))

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
          (swap! cluster-state assoc-in [:nodes node] :normal)
          ;(future (distribute-state nodes))
          {:status 201
           :body {:state @cluster-state}})))))

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
      (loop [node-item (get-random-node me)]
        (if (nil? node-item)
          (do
            (log/debugf "No other nodes in the cluster...%s" (:nodes @cluster-state))
            (Thread/sleep interval)
            (recur (get-random-node me)))
          (let [node (first node-item)
                res (ping-node node)]
            (if (nil? res)
              (do
                (inc-node-failure node)
                (log/info "!!fail" @ping-failures)
                (if (> (get @ping-failures node 0) @failure-threshold)
                  (down node))))
            (Thread/sleep interval)
            (recur (get-random-node me)))))
      (catch Exception e
        (log/error e "blah")))))

(defn flush-state-to-disk
  [interval]
  (future
    (while true
      (log/debugf "Flush state to disk")
      (spit "state.clj" @cluster-state)
      (Thread/sleep interval))))

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
      :ping (ping remote-addr args)
      (ping))))

(def app
  (-> handler
      middleware/wrap-edn-body
      middleware/wrap-no-routes
      (middleware/wrap-content-type "application/edn")))

(defn set-failure-threshold
  [threshold]
  (log/infof "Set failure detection threshold (%s)" threshold)
  (reset! failure-threshold threshold))

(defn start
  [conf]
  (let [host (:host conf)
        port (:port conf)
        interval (:interval conf)]
    (log/infof "Starting gossip listener at %s:%s" host port)
    (if (and (:load-state conf) (.exists (as-file "state.clj")))
      (dosync
        (reset! cluster-state (read-string (slurp "state.clj")))
        (inc-version)))
    (set-failure-threshold (:failure-threshold conf))
    (heartbeat host interval)
    (flush-state-to-disk (:flush-interval conf))
    (future (jetty/run-jetty app conf))))
