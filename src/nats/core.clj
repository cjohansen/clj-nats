(ns nats.core
  (:require [nats.message :as message])
  (:import (io.nats.client Nats)))

(defn ^:export connect [uri]
  (Nats/connect uri))

(defn ^:export close [conn]
  (.close conn))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message. Performs no publish acking; do not use for publishing to a
  JetStream subject, instead use `nats.jet-stream/publish`.

  message is a map of:

  - `:subject` - The subject to publish to
  - `:data` - The message data. Can be any Clojure value
  - `:headers` - An optional map of string keys to string (or collection of
                 string) values to set as meta-data on the message.
  - `:reply-to` - An optional reply-to subject."
  [conn message]
  (assert (not (nil? (:subject message))) "Can't publish without data")
  (assert (not (nil? (:data message))) "Can't publish nil data")
  (->> (message/build-message message)
       (.publish conn)))
