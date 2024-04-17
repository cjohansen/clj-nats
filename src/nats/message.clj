(ns nats.message
  (:require [clojure.set :as set])
  (:import (io.nats.client Message)
           (io.nats.client.api MessageInfo PublishAck)
           (io.nats.client.impl AckType Headers NatsJetStreamMetaData NatsMessage
                                NatsMessage$Builder)
           (io.nats.client.support Status)))

(def ack-types
  {:nats.ack-type/ack AckType/AckAck
   :nats.ack-type/nak AckType/AckNak
   :nats.ack-type/progress AckType/AckProgress
   :nats.ack-type/term AckType/AckTerm
   :nats.ack-type/next AckType/AckNext})

(def ^:no-doc ack-type->k (set/map-invert ack-types))

(defn ^:no-doc map->Headers [headers]
  (let [headers-obj ^Headers (Headers.)]
    (doseq [[k v] headers]
      (->> (cond-> v
             (not (coll? v)) vector)
           (map str)
           (.add headers-obj (name k))))
    headers-obj))

(defn ^:no-doc headers->map [^Headers headers]
  (when headers
    (->> (for [k (.keySet headers)]
           [k (.get headers k)])
         (into {}))))

(defn ^:no-doc get-message-kind [headers data]
  (cond
    (or (get headers "content-type") (bytes? data))
    ::bytes

    (string? data)
    ::string

    :else
    ::edn))

(defn build-message [{::keys [subject headers data reply-to]}]
  (let [kind (get-message-kind headers data)
        headers (cond-> headers
                  (= ::string kind) (assoc "content-type" ["text/plain"])
                  (= ::edn kind) (assoc "content-type" ["application/edn"]))]
    (cond-> ^NatsMessage$Builder (NatsMessage/builder)
      subject (.subject subject)
      reply-to (.replyTo reply-to)
      headers (.headers (map->Headers headers))
      data (.data (cond-> data
                    (= ::edn kind) pr-str))
      :always (.build))))

(defn ^:no-doc bytes->edn [data]
  (let [s (String. data)]
    (try
      (read-string s)
      (catch Exception e
        (throw (ex-info "Unable to parse body of EDN message" {:data s} e))))))

(defn ^:no-doc get-message-data [headers data]
  (let [content-type (first (get headers "content-type"))]
    (cond-> data
      (= "text/plain" content-type) (String.)
      (= "application/edn" content-type) bytes->edn)))

(defn ^:no-doc message-info->map [^MessageInfo message]
  (let [headers (headers->map (.getHeaders message))]
    {::data (get-message-data headers (.getData message))
     ::headers headers
     ::last-seq (.getLastSeq message)
     ::seq (.getSeq message)
     ::stream (.getStream message)
     ::subject (.getSubject message)
     ::received-at (.toInstant (.getTime message))}))

(defn ^:no-doc status->map [^Status status]
  (when status
    {:nats.status/code (.getCode status)
     :nats.status/message (.getMessage status)
     :nats.status/message-with-code (.getMessageWithCode status)
     :nats.status/flow-control? (.isFlowControl status)
     :nats.status/heartbeat? (.isHeartbeat status)
     :nats.status/no-responders? (.isNoResponders status)}))

(defn ^:no-doc jet-stream-metadata->map [^NatsJetStreamMetaData metadata]
  (let [domain (.getDomain metadata)]
    (cond-> {:nats.stream.meta/stream (.getStream metadata)
             :nats.stream.meta/consumer (.getConsumer metadata)
             :nats.stream.meta/delivered-count (.deliveredCount metadata)
             :nats.stream.meta/stream-sequence (.streamSequence metadata)
             :nats.stream.meta/consumer-sequence (.consumerSequence metadata)
             :nats.stream.meta/pending-count (.pendingCount metadata)
             :nats.stream.meta/timestamp (.toInstant (.timestamp metadata))}
      domain (assoc :domain domain))))

(defn message->map [^Message message]
  (let [headers (headers->map (.getHeaders message))
        status (status->map (.getStatus message))
        last-ack (.lastAck message)
        jet-stream? (.isJetStream message)]
    (cond-> {::consume-byte-count (.consumeByteCount message)
             ::data (get-message-data headers (.getData message))
             ::headers headers
             ::reply-to (.getReplyTo message)
             ::SID (.getSID message)
             ::subject (.getSubject message)
             ::has-headers? (.hasHeaders message)
             ::jet-stream? jet-stream?
             ::status-message? (.isStatusMessage message)}
      status (assoc ::status status)
      last-ack (assoc ::last-ack (ack-type->k last-ack))
      jet-stream? (assoc ::metadata (jet-stream-metadata->map (.metaData message))))))

(defn ^:no-doc publish-ack->map [^PublishAck ack]
  (when ack
    (let [domain (.getDomain ack)]
      (cond-> {:nats.publish-ack/seq-no (.getSeqno ack)
               :nats.publish-ack/stream (.getStream ack)
               :nats.publish-ack/duplicate? (.isDuplicate ack)}
        domain (assoc :nats.publish-ack/domain domain)))))
