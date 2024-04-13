(ns nats.core
  (:require [nats.message :as message])
  (:import (io.nats.client Nats Subscription)
           (java.time ZoneId)))

(def default-tz
  "The Java SDK uses ZonedDateTime for every instant and defaults the time zone to
   GMT. All the NATS times are instants in time, so Instant is the appropriate
   representation for them - no need to wrap them all in a timezone. This default
   timezone is here only to convert incoming Instants to the ZonedDateTime the Java
   SDK expects."
  (ZoneId/of "GMT"))

(defn ^:export connect [uri]
  (Nats/connect uri))

(defn ^:export close [conn]
  (.close conn))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message. Performs no publish acking; do not use for publishing to a
  JetStream subject, instead use `nats.stream/publish`.

  `message` is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.
  - `:nats.message/reply-to` - An optional reply-to subject."
  [conn message]
  (assert (not (nil? (::message/subject message))) "Can't publish without data")
  (assert (not (nil? (::message/data message))) "Can't publish nil data")
  (->> (nats.message/build-message message)
       (.publish conn)
       message/publish-ack->map))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to non-stream subject. For JetStream subjects, instead use
  `nats.stream/subscribe`. Pull messages with `nats.core/pull-message`."
  [conn subject & [queue-name]]
  (if queue-name
    (.subscribe conn subject queue-name)
    (.subscribe conn subject)))

(defn ^:export pull-message [^Subscription subscription timeout]
  (some-> (.nextMessage subscription timeout) message/message->map))

(defn ^:export unsubscribe [^Subscription subscription]
  (.unsubscribe subscription)
  nil)

(defn ^{:style/indent 1 :export true} request
  "Make a request and wait for the response. Returns a future that resolves with
  the response.

  `message` is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.

  In request/response, `:nats.message/reply-to` is reserved for the server."
  [conn message]
  (assert (not (nil? (::message/subject message))) "Can't publish without data")
  (assert (not (nil? (::message/data message))) "Can't publish nil data")
  (future
    (->> (nats.message/build-message (dissoc message :nats.message/reply-to))
         (.request conn)
         deref
         message/message->map)))
