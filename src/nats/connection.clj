(ns nats.connection
  (:require [clojure.core.protocols :as cp]
            [nats.protocols :as p])
  (:import (io.nats.client Connection)))

(defn Connection->map [^Connection conn]
  (-> (bean conn)
      (update :clientInetAddress bean)
      (update :serverInfo bean)
      (update :statistics bean)
      (update :status bean)
      (update :options bean)))

(deftype Conn [^Connection conn configuration state]
  p/JNatsConnectionWrapper
  (get-jnats-conn [_]
    conn)

  p/NatsConnConfig
  (get-configuration [_]
    @configuration)

  (configure! [self k v]
    (let [path (if (vector? k) k [k])]
      (if (nil? v)
        (if (= 1 (count path))
          (swap! configuration dissoc k)
          (swap! configuration update-in (butlast path) dissoc (last path)))
        (swap! configuration assoc-in path v)))
    self)

  cp/Datafiable
  (datafy [_]
    {:conn (Connection->map conn)
     :configuration (select-keys @configuration [:edn-reader-opts
                                                 :jet-stream-options
                                                 :key-value-options])})

  Object
  (toString [_]
    (or (.getConnectedUrl conn) "<disconnected>"))

  java.lang.AutoCloseable
  (close [_]
    (.close conn)))

(defn make-connection [jnats-conn configuration]
  (->Conn jnats-conn (atom configuration) (atom {})))
