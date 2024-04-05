(ns nats.core
  (:import (io.nats.client Nats)
           (io.nats.client.impl Headers NatsMessage NatsMessage$Builder)))

(defn ^:export connect [uri]
  (Nats/connect uri))

(defn ^:export close [conn]
  (.close conn))

(defn map->Headers [headers]
  (let [headers-obj ^Headers (Headers.)]
    (for [[k v] headers]
      (->> (cond-> v
             (not (coll? v)) vector)
           (map str)
           (.add headers-obj (name k))))
    headers-obj))

(defn ^{:style/indent 1 :export true} publish [conn {:keys [subject headers data reply-to]}]
  (->> (cond-> ^NatsMessage$Builder (NatsMessage/builder)
         subject (.subject subject)
         reply-to (.replyTo reply-to)
         headers (.headers (map->Headers headers))
         data (.data (cond-> data
                       (not (string? data)) pr-str))
         :always (.build))
       (.publish (.jetStream conn))))
