(ns kraken.server
  (:require [clojure.tools.logging :as log]
            [ring.adapter.jetty :as jetty]
            [kraken.cluster :as cluster]
            [kraken.middleware :as middleware]))

(defn handler
  [request]
  (let [body (:body request)
        remote-addr (:remote-addr request)
        command (first body)
        args (rest body)]
    (condp = command
      :join (cluster/join remote-addr args)
      :leave (cluster/leave remote-addr args)
      :state (cluster/state remote-addr args)
      :elect (cluster/elect args)
      :ping (cluster/ping)
      (cluster/ping))))

(def app
  (-> handler
      middleware/wrap-edn-body
      middleware/wrap-no-routes
      (middleware/wrap-content-type "application/edn")))

(defn start
  [options]
  (let [conf {:host (:host options)
              :port (:port options)
              :join false}]
    (log/infof "Starting cluster node %s:%s" (:host conf) (:port conf))
    (jetty/run-jetty app conf)))
