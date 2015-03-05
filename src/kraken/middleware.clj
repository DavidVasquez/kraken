(ns kraken.middleware)

(def bad-request {:status 400
                  :body "Bad Request"})

(def not-found {:status 404
                :body "Not Found"})

(defn wrap-content-type
  [handler content-type]
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Content-Type"] content-type))))

(defn wrap-edn-body
  [handler]
  (fn [request]
    (let [body (slurp (:body request))]
      (if (empty? body)
        bad-request
        (let [edn-body (read-string body)
              response (handler (assoc request :body edn-body))]
          (assoc response :body (pr-str (:body response))))))))

(defn wrap-no-routes
  [handler]
  (fn [request]
    (if (= 1 (count (:uri request)))
      (handler request)
      not-found)))
