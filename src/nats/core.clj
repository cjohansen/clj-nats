(ns nats.core
  (:import (io.nats.client Nats)
           (io.nats.client.api MessageInfo)
           (io.nats.client.impl Headers NatsMessage NatsMessage$Builder)))

(defn map->Headers [headers]
  (let [headers-obj ^Headers (Headers.)]
    (doseq [[k v] headers]
      (->> (cond-> v
             (not (coll? v)) vector)
           (map str)
           (.add headers-obj (name k))))
    headers-obj))

(defn headers->map [^Headers headers]
  (when headers
    (->> (for [k (.keySet headers)]
           [k (.get headers k)])
         (into {}))))

(defn get-message-kind [headers data]
  (cond
    (or (get headers "content-type")
        (.isArray (class data)))
    :bytes

    (string? data)
    :string

    :else
    :edn))

(defn build-message [{:keys [subject headers data reply-to]}]
  (let [kind (get-message-kind headers data)
        headers (cond-> headers
                  (= :string kind) (assoc "content-type" ["text/plain"])
                  (= :edn kind) (assoc "content-type" ["application/edn"]))]
    (cond-> ^NatsMessage$Builder (NatsMessage/builder)
      subject (.subject subject)
      reply-to (.replyTo reply-to)
      headers (.headers (map->Headers headers))
      data (.data (cond-> data
                    (= :edn kind) pr-str))
      :always (.build))))

(defn bytes->edn [data]
  (read-string (String. data)))

(defn message-info->map [^MessageInfo message]
  (let [headers (headers->map (.getHeaders message))
        content-type (first (get headers "content-type"))]
    {:data (cond-> (.getData message)
             (= "text/plain" content-type) (String.)
             (= "application/edn" content-type) bytes->edn)
     :headers headers
     :last-seq (.getLastSeq message)
     :seq (.getSeq message)
     :stream (.getStream message)
     :subject (.getSubject message)
     :time (.getTime message)}))

(defn ^:export connect [uri]
  (Nats/connect uri))

(defn ^:export close [conn]
  (.close conn))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message. Performs no publish acking; do not use for publishing to a
  JetStream subject, instead use `nats.stream/publish`.

  message is a map of:

  - `:subject` - The subject to publish to
  - `:data` - The message data. Can be any Clojure value
  - `:headers` - An optional map of string keys to string (or collection of
                 string) values to set as meta-data on the message.
  - `:reply-to` - An optional reply-to subject."
  [conn message]
  (assert (not (nil? (:subject message))) "Can't publish without data")
  (assert (not (nil? (:data message))) "Can't publish nil data")
  (->> (build-message message)
       (.publish conn)))
