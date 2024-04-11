(ns nats.core
  (:import (io.nats.client Message Nats Subscription)
           (io.nats.client.api MessageInfo PublishAck)
           (io.nats.client.impl AckType Headers NatsJetStreamMetaData
                                NatsMessage NatsMessage$Builder)
           (io.nats.client.support Status)))

(def ack-types
  {:nats.ack-type/ack AckType/AckAck
   :nats.ack-type/nak AckType/AckNak
   :nats.ack-type/progress AckType/AckProgress
   :nats.ack-type/term AckType/AckTerm
   :nats.ack-type/next AckType/AckNext})

(def ack-type->k
  (into {} (map (juxt second first) ack-types)))

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
      subject (.subject (name subject))
      reply-to (.replyTo reply-to)
      headers (.headers (map->Headers headers))
      data (.data (cond-> data
                    (= :edn kind) pr-str))
      :always (.build))))

(defn bytes->edn [data]
  (read-string (String. data)))

(defn get-message-data [headers data]
  (let [content-type (first (get headers "content-type"))]
    (cond-> data
      (= "text/plain" content-type) (String.)
      (= "application/edn" content-type) bytes->edn)))

(defn message-info->map [^MessageInfo message]
  (let [headers (headers->map (.getHeaders message))]
    {:data (get-message-data headers (.getData message))
     :headers headers
     :last-seq (.getLastSeq message)
     :seq (.getSeq message)
     :stream (.getStream message)
     :subject (.getSubject message)
     :time (.getTime message)}))

(defn status->map [^Status status]
  (when status
    {:code (.getCode status)
     :message (.getMessage status)
     :message-with-code (.getMessageWithCode status)
     :flow-control? (.isFlowControl status)
     :heartbeat? (.isHeartbeat status)
     :no-responders? (.isNoResponders status)}))

(defn jet-stream-metadata->map [^NatsJetStreamMetaData metadata]
  (let [domain (.getDomain metadata)]
    (cond-> {:stream (.getStream metadata)
             :consumer (.getConsumer metadata)
             :delivered-count (.deliveredCount metadata)
             :stream-sequence (.streamSequence metadata)
             :consumer-sequence (.consumerSequence metadata)
             :pending-count (.pendingCount metadata)
             :timestamp (.toInstant (.timestamp metadata))}
      domain (assoc :domain domain))))

(defn message->map [^Message message]
  (let [headers (headers->map (.getHeaders message))
        status (status->map (.getStatus message))
        last-ack (.lastAck message)
        jet-stream? (.isJetStream message)]
    (cond-> {:consume-byte-count (.consumeByteCount message)
             :data (get-message-data headers (.getData message))
             :headers headers
             :reply-to (.getReplyTo message)
             :SID (.getSID message)
             :subject (.getSubject message)
             :has-headers? (.hasHeaders message)
             :jet-stream? jet-stream?
             :status-message? (.isStatusMessage message)}
      status (assoc :status status)
      last-ack (assoc :last-ack (ack-type->k last-ack))
      jet-stream? (assoc :metadata (jet-stream-metadata->map (.metaData message))))))

(defn publish-ack->map [^PublishAck ack]
  (when ack
    {:domain (.getDomain ack)
     :seq-no (.getSeqno ack)
     :stream (.getStream ack)
     :duplicate? (.isDuplicate ack)}))

(defn ^:export connect [uri]
  (Nats/connect uri))

(defn ^:export close [conn]
  (.close conn))

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
       (.publish conn)
       publish-ack->map))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to non-stream subject. For JetStream subjects, instead use
  `nats.stream/subscribe`. Pull messages with `nats.core/pull-message`."
  [conn subject & [queue-name]]
  (if queue-name
    (.subscribe conn (name subject) queue-name)
    (.subscribe conn (name subject))))

(defn ^:export pull-message [^Subscription subscription timeout]
  (some-> (.nextMessage subscription timeout) message->map))

(defn ^:export unsubscribe [^Subscription subscription]
  (.unsubscribe subscription)
  nil)
