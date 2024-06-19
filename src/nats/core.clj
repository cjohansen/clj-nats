(ns nats.core
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [nats.message :as message])
  (:import (io.nats.client Nats
                           ErrorListener
                           Subscription)
           (java.time ZoneId)))

(def ^:no-doc connections (atom {}))

(def ^:no-doc default-tz
  "The Java SDK uses ZonedDateTime for every instant and defaults the time zone to
   GMT. All the NATS times are instants in time, so Instant is the appropriate
   representation for them - no need to wrap them all in a timezone. This default
   timezone is here only to convert incoming Instants to the ZonedDateTime the Java
   SDK expects."
  (ZoneId/of "GMT"))

(defn- covers? [haystacks needles]
  (if (< (count needles) (count haystacks))
    false
    (loop [[haystack & haystacks] haystacks
           [needle & needles] needles]
      (cond
        (and (empty? haystack) (empty? needle))
        true

        (= haystack needle)
        (recur haystacks needles)

        (and (= "*" haystack) needle)
        (recur haystacks needles)

        (and (= ">" haystack) needle)
        true))))

(defn ^:no-doc covers-subject? [patterns subject]
  (let [s-pieces (str/split subject #"\.")]
    (boolean (some #(covers? (str/split % #"\.") s-pieces) patterns))))

(defn ^:export create-error-listener
  "Create an `io.nats.client.ErrorListener` instance. Takes the following
  functions:

  - `:error-occurred` `(fn [conn error])`
  - `:exception-occurred` `(fn [conn exception])`
  - `:flow-control-processed`  `(fn [conn subscription subject source])`
  - `:heartbeat-alarm`  `(fn [conn subscription last-stream-seq last-consumer-seq])`
  - `:message-discarded`  `(fn [conn message])`
  - `:pull-status-error`  `(fn [conn subscription status])`
  - `:pull-status-warning`  `(fn [conn subscription status])`
  - `:slow-consumer-detected`  `(fn [conn consumer])`
  - `:socket-write-timeout`  `(fn [conn])`
  - `:supply-message`  `(fn [label conn consumer sub pairs])`
  - `:unhandled-status`  `(fn [conn subscription status])`"
  [{:keys [error-occurred
           exception-occurred
           flow-control-processed
           heartbeat-alarm
           message-discarded
           pull-status-error
           pull-status-warning
           slow-consumer-detected
           socket-write-timeout
           supply-message
           unhandled-status]}]
  (reify ErrorListener
    (errorOccurred [_this conn error]
      (when (ifn? error-occurred)
        (error-occurred (get @connections conn) error)))

    (exceptionOccurred [_this conn exception]
      (when (ifn? exception-occurred)
        (exception-occurred (get @connections conn) exception)))

    (flowControlProcessed [_this conn subscription subject source]
      (when (ifn? flow-control-processed)
        (flow-control-processed (get @connections conn) subscription subject source)))

    (heartbeatAlarm [_this conn subscription last-stream-seq last-consumer-seq]
      (when (ifn? heartbeat-alarm)
        (heartbeat-alarm (get @connections conn) subscription last-stream-seq last-consumer-seq)))

    (messageDiscarded [_this conn message]
      (when (ifn? message-discarded)
        (message-discarded (get @connections conn) (message/message->map message))))

    (pullStatusError [_this conn subscription status]
      (when (ifn? pull-status-error)
        (pull-status-error (get @connections conn) subscription (message/status->map status))))

    (pullStatusWarning [_this conn subscription status]
      (when (ifn? pull-status-warning)
        (pull-status-warning (get @connections conn) subscription (message/status->map status))))

    (slowConsumerDetected [_this conn consumer]
      (when (ifn? slow-consumer-detected)
        (slow-consumer-detected (get @connections conn) consumer)))

    (socketWriteTimeout [_this conn]
      (when (ifn? socket-write-timeout)
        (socket-write-timeout (get @connections conn))))

    (supplyMessage [_this label conn consumer sub pairs]
      (when (ifn? supply-message)
        (supply-message label conn consumer sub pairs)))

    (unhandledStatus [_this conn subscription status]
      (when (ifn? unhandled-status)
        (unhandled-status (get @connections conn) subscription (message/status->map status))))))

(defn ^:export connect
  "Connect to the NATS server. Optionally configure jet stream and key/value
  management, or use `nats.stream/configure` and `nats.kv/configure`
  respectively later."
  [uri & [{:keys [jet-stream-options key-value-options]}]]
  (let [conn (Nats/connect uri)
        clj-conn (atom {:conn conn
                        :jet-stream-options jet-stream-options
                        :key-value-options key-value-options})]
    (swap! connections assoc conn clj-conn)
    clj-conn))

(defn ^:no-doc get-connection [conn]
  (:conn @conn))

(defn ^:export close [conn]
  (let [jconn (get-connection conn)]
    (swap! connections dissoc jconn)
    (.close jconn)))

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
  (->> (message/build-message message)
       (.publish (get-connection conn))
       message/publish-ack->map))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to non-stream subject. For JetStream subjects, instead use
  `nats.stream/subscribe`. Pull messages with `nats.core/pull-message`."
  [conn subject & [queue-name]]
  (if queue-name
    (.subscribe (get-connection conn) subject queue-name)
    (.subscribe (get-connection conn) subject)))

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
    (->> (message/build-message (dissoc message :nats.message/reply-to))
         (.request (get-connection conn))
         deref
         message/message->map)))
