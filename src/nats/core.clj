(ns nats.core
  (:require [clojure.string :as str]
            [nats.message :as message])
  (:import (io.nats.client Nats Subscription)
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
