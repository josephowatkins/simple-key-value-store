(ns simple-key-value-store.handler
  (:require [compojure.core :refer [defroutes POST GET]]
            [compojure.route :as route]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.util.response :refer [response]]
            [simple-key-value-store.core :as core]))


(defroutes handler
  (GET "/get" [key]
    (when-let [value (core/get key)]
      (response value)))
  (POST "/set" [key value]
    (core/set key value)
    (response ""))
  (route/not-found ""))

(def app
  (-> handler
      (wrap-defaults api-defaults)
      wrap-json-response
      wrap-keyword-params
      wrap-json-params))
