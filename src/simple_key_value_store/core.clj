(ns simple-key-value-store.core
  (:refer-clojure :exclude [get set read])
  (:require [clojure.java.io :refer [file make-parents]])
  (:import (java.io RandomAccessFile FileOutputStream DataOutputStream File)))

(def data-directory "data")
(def counter (atom 0))

(def writeable-block (atom nil))

(defn read-from-file [fname offset]
  (let [raf (RandomAccessFile. fname "r")]
    (.seek raf offset)
    (let [result (.readUTF raf)]
      (.close raf)
      result)))

(defn append-to-file [fname s]
  (let [bytes (.getBytes s)]
    (with-open [dos (DataOutputStream. (FileOutputStream. fname true))]
      (.writeUTF dos s))))

(defn create-file! [fname]
  (.createNewFile (File. fname)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; protocols

(defprotocol ReadableBlock
  (read-key [this key]))

(defprotocol WriteableBlock
  (write-key [this key value])
  (delete-key [this key]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; types

(defrecord Block [fname offsets]
  ReadableBlock
  (read-key [_ key]
    (when-let [offset (clojure.core/get offsets key)]
      (read-from-file fname offset)))

  WriteableBlock
  (write-key [this key value]
    (let [offset (.length (File. fname))
          payload (str {key value})]
      (println offset)
      (append-to-file fname payload)
      (->Block fname (assoc offsets key offset))))

  (delete-key [this key]
    (throw (Exception. "function not implemented"))))

(defn new-block
  "create a new storage block on disk, returns the in memory representation"
  [counter-ref]
  (let [fname (str data-directory "/f" (swap! counter inc))]
    (create-file! fname)
    (->Block fname {})))

(defn merge-blocks [block1 block2]
  (throw (Exception. "function not implement")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; public api

(defn get [key]
  ;; TODO: tidy up?
  (read-key @writeable-block key))

(defn set [key value]
  (swap! writeable-block write-key key value))

(defn delete [key]
  (throw (Exception. "function not implemented")))

(defn init-db []
  (.mkdir (File. data-directory))
  (let [block (new-block counter)]
    (reset! writeable-block block)))
