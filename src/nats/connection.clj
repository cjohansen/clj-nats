(ns nats.connection
  (:require [clojure.core.protocols :as cp]
            [nats.internals :as internals]
            [nats.protocols :as p])
  (:import (io.nats.client Connection)))

(defn Connection->map [^Connection conn]
  (-> (bean conn)
      (update :clientInetAddress bean)
      (update :serverInfo bean)
      (update :statistics bean)
      (update :status bean)
      (update :options bean)))

(defn atom-set! "assoc, assoc-in and dissoc-in in one package"
  [atom k v]
  (let [path (if (vector? k) k [k])]
    (if (nil? v)
      (if (= 1 (count path))
        (swap! atom dissoc k)
        (swap! atom update-in (butlast path) dissoc (last path)))
      (swap! atom assoc-in path v))))

(deftype Conn [^Connection conn configuration state]
  p/JNatsConnectionWrapper
  (get-jnats-conn [_]
    conn)

  p/NatsConnConfig
  (get-configuration [_]
    @configuration)

  (configure! [self k v]
    (atom-set! configuration k v)
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
    (->> (:nats.object-store/subscriptions @state)
         (mapcat vals)
         (run! java.lang.AutoCloseable/.close))
    (swap! state dissoc :nats.object-store/subscriptions)
    (.close conn))

  internals/NatsConnectionState
  (get-state [_] @state)
  (set-state! [self k v]
    (atom-set! state k v)
    self))

(defn make-connection [jnats-conn configuration]
  (->Conn jnats-conn (atom configuration) (atom {})))
