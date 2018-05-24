(ns simple-key-value-store.main
  (:require [clojure.tools.logging :as log]
            [ring.adapter.jetty :refer [run-jetty]]
            [simple-key-value-store.handler :as handler]
            [simple-key-value-store.core :as core]))

(defn -main [& args]
  (log/info "starting db on port: 7654")
  (core/init-db)
  (run-jetty handler/app {:port 7654}))
