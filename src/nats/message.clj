(ns nats.message
  (:import (io.nats.client.impl Headers NatsMessage NatsMessage$Builder)))

(defn map->Headers [headers]
  (let [headers-obj ^Headers (Headers.)]
    (for [[k v] headers]
      (->> (cond-> v
             (not (coll? v)) vector)
           (map str)
           (.add headers-obj (name k))))
    headers-obj))

(defn ^:export build-message [{:keys [subject headers data reply-to]}]
  (cond-> ^NatsMessage$Builder (NatsMessage/builder)
    subject (.subject subject)
    reply-to (.replyTo reply-to)
    headers (.headers (map->Headers headers))
    data (.data (cond-> data
                  (not (string? data)) pr-str))
    :always (.build)))
