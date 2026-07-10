(ns nats.protocols
  (:import (io.nats.client Connection)
           (java.io Writer)))

(defprotocol JNatsConnectionWrapper
  (get-jnats-conn ^io.nats.client.Connection [self]))

(defprotocol NatsConnConfig
  (get-configuration [self])
  (configure! [self k v]))

(defmethod print-method nats.protocols.JNatsConnectionWrapper
  [conn writer]
  (let [^Connection jnats-conn (get-jnats-conn conn)]
    (Writer/.write writer "#nats/connection ")
    (Writer/.write writer (pr-str (or (.getConnectedUrl jnats-conn)
                                      "<disconnected>")))))
