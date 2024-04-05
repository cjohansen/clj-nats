(ns nats.core
  (:import (io.nats.client Nats)))

(defn connect [uri]
  (Nats/connect uri))

(defn close [conn]
  (.close conn))
