(ns nats.protocols)

(defprotocol JNatsConnectionWrapper
  (get-jnats-conn ^io.nats.client.Connection [self]))

(defprotocol NatsConnConfig
  (get-configuration [self])
  (configure! [self k v]))
