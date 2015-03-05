(ns kraken.gossip
  (:require [clojure.tools.logging :as log]
            [kraken.http :as http]))

(defn distribute-state
  [nodes]
  (log/infof "Distribute state %s" @nodes)
  (pmap (fn [[node args]]
          (let [url (str "http://" node ":10001")]
            (http/post url [:state @nodes])))
        @nodes))

(defn ping
  ""
  [node]
  (let [url (str "http://" node ":10001")
        response (try
                   (http/post url [:ping])
                   (catch Exception e
                     nil))]
    (:body response)))

;(defn send-to-node)
