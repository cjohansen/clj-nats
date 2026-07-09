(ns nats.connection
  (:require [nats.protocols :as p])
  (:import (io.nats.client Connection)))

(deftype Conn [^Connection conn configuration]
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

  java.lang.AutoCloseable
  (close [_]
    (.close conn)))
