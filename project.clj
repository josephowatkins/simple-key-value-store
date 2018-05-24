(defproject simple-key-value-store "SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.4.1"]
                 ;; webserver
                 [compojure "1.6.1"]
                 [ring "1.6.3"]
                 [ring/ring-defaults "0.3.1"]
                 [ring/ring-json "0.4.0"]]

  :main ^:skip-aot simple-key-value-store.main

  :profiles {:uberjar {:aot [simple-key-value-store.main]
                       :uberjar-name "app.jar"}})
