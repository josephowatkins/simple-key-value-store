(ns simple-key-value-store.core
  (:refer-clojure :exclude [get set merge])
  (:require [clojure.java.io :refer [file make-parents]]
            [clojure.data.json :as json])
  (:import (java.io RandomAccessFile
                    FileOutputStream
                    FileInputStream
                    DataOutputStream
                    DataInputStream
                    File)))

(def default-block-size 1024)

(def data-directory "data")
(def counter (atom 0))

(def writeable-block (atom nil))

(defn read-all [fname]
  (letfn [(read-stream [s]
            (when (> (.available s) 0)
              (cons (json/read-str (.readUTF s)) (read-stream s))))]
    (with-open [dis (DataInputStream. (FileInputStream. fname))]
      (read-stream dis))))

(defn read-from-file [fname offset]
  (let [raf (RandomAccessFile. fname "r")]
    (.seek raf offset)
    (let [result (.readUTF raf)]
      (.close raf)
      (json/read-str result))))

(defn append-to-file [fname s] ;; could just be spit
  (let [bytes (.getBytes s)]
    (with-open [dos (DataOutputStream. (FileOutputStream. fname true))]
      (.writeUTF dos s))))

(defn create-file! [fname]
  (.createNewFile (File. fname)))

(defn gen-fname!
  "this is wrong! good block naming scheme?"
  []
  (str data-directory "/f" (swap! counter inc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; protocols

(defprotocol ReadableBlock
  (read-key [this key]))

(defprotocol WriteableBlock
  (write-key [this key value])
  (delete-key [this key]))

(defprotocol MergableBlock
  (merge [this block]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; types

(defrecord Block [fname offsets]
  ReadableBlock
  (read-key [this key]
    (when-let [offset (clojure.core/get offsets key)]
      (read-from-file fname offset)))

  WriteableBlock
  (write-key [this key value]
    (let [offset (.length (File. fname))
          payload (str {key value})]
      (append-to-file fname payload)
      (->Block fname (assoc offsets key offset))))

  (delete-key [this key]
    (throw (Exception. "function not implemented")))

  MergableBlock
  (merge [this block]
    (let [kvs (reduce clojure.core/merge
                      {}
                      (concat (read-all this) (read-all block)))]
      (create-file! (gen-fname!))
      (let [fname (gen-fname!)]
        (reduce (fn [acc [k v]]
                  (write-key acc k v))
                (->Block fname {})
                kvs)))))

(defn new-block
  "create a new storage block on disk, returns the in memory representation"
  []
  (let [fname (gen-fname!)]
    (create-file! fname)
    (->Block fname {})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; public api

(defn get [key]
  (read-key @writeable-block key))

(defn set [key value]
  (swap! writeable-block write-key key value))

(defn delete [key]
  (throw (Exception. "function not implemented")))

(defn init-db []
  (.mkdir (File. data-directory))
  (let [block (new-block counter)]
    (reset! writeable-block block)))
