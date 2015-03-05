(ns kraken.http
  (:require [clj-http.client :as client]))

(defn post
  [url body]
  (let [request {:conn-timeout 3000
                 :content-type "application/edn"
                 :body (pr-str body)}
        response (client/post url request)]
    (assoc response :body (read-string (:body response)))))
