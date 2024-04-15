(ns nats.consumer
  (:require [clojure.set :as set]
            [nats.cluster :as cluster]
            [nats.core :as nats]
            [nats.message :as message]
            [nats.stream :as stream])
  (:import (io.nats.client ConsumeOptions ConsumeOptions$Builder IterableConsumer)
           (io.nats.client.api AckPolicy ConsumerConfiguration ConsumerConfiguration$Builder ConsumerInfo DeliverPolicy ReplayPolicy)
           (io.nats.client.impl AckType)
           (java.time Instant)))

;; Enums as keywords

(def ack-policies
  "Available acknowledgment policies. One of:

   - `:nats.ack-policy/all`
   - `:nats.ack-policy/explicit`
   - `:nats.ack-policy/none`"
  {:nats.ack-policy/all AckPolicy/All
   :nats.ack-policy/explicit AckPolicy/Explicit
   :nats.ack-policy/none AckPolicy/None})

(def ^:no-doc ack-policy->k (set/map-invert ack-policies))

(def deliver-policies
  "Available deliver policies. One of:

   - `:nats.deliver-policy/all`
   - `:nats.deliver-policy/by-start-sequence`
   - `:nats.deliver-policy/by-start-time`
   - `:nats.deliver-policy/last`
   - `:nats.deliver-policy/last-per-subject`
   - `:nats.deliver-policy/new`"
  {:nats.deliver-policy/all DeliverPolicy/All
   :nats.deliver-policy/by-start-sequence DeliverPolicy/ByStartSequence
   :nats.deliver-policy/by-start-time DeliverPolicy/ByStartTime
   :nats.deliver-policy/last DeliverPolicy/Last
   :nats.deliver-policy/last-per-subject DeliverPolicy/LastPerSubject
   :nats.deliver-policy/new DeliverPolicy/New})

(def ^:no-doc deliver-policy->k (set/map-invert deliver-policies))

(def replay-policies
  "Available replay policies. One of:

   - `:nats.replay-policy/instant`
   - `:nats.replay-policy/original`"
  {:nats.replay-policy/instant ReplayPolicy/Instant
   :nats.replay-policy/original ReplayPolicy/Original})

(def ^:no-doc replay-policy->k (set/map-invert replay-policies))

;; Map data classes to maps

(defn ^:no-doc consumer-configuration->map [^ConsumerConfiguration config]
  (let [filter-subject (.getFilterSubject config)]
    (cond-> {::ack-policy-was-set? (.ackPolicyWasSet config)
             ::backoff-was-set? (.backoffWasSet config)
             ::deliver-policy-was-set? (.deliverPolicyWasSet config)
             ::flow-control-was-set? (.flowControlWasSet config)
             ::ack-policy (ack-policy->k (.getAckPolicy config))
             ::ack-wait (.getAckWait config)
             ::backoff (seq (.getBackoff config))
             ::deliver-group (.getDeliverGroup config)
             ::deliver-policy (deliver-policy->k (.getDeliverPolicy config))
             ::deliver-subject (.getDeliverSubject config)
             ::description (.getDescription config)
             ::durable (.getDurable config)
             ::filter-subjects (.getFilterSubjects config)
             ::idle-heartbeat (.getIdleHeartbeat config)
             ::inactve-threshold (.getInactiveThreshold config)
             ::max-ack-pending (.getMaxAckPending config)
             ::max-batch (.getMaxBatch config)
             ::max-bytes (.getMaxBytes config)
             ::max-deliver (.getMaxDeliver config)
             ::max-expires (.getMaxExpires config)
             ::max-pull-waiting (.getMaxPullWaiting config)
             ::metadata (into {} (.getMetadata config))
             ::consumer-name (.getName config)
             ::num-replicas (.getNumReplicas config)
             ::pause-until (.getPauseUntil config)
             ::rate-limit (.getRateLimit config)
             ::replay-policy (replay-policy->k (.getReplayPolicy config))
             ::sample-frequency (.getSampleFrequency config)
             ::start-sequence (.getStartSequence config)
             ::start-time (some-> (.getStartTime config) .toInstant)
             ::has-multiple-filter-subjects? (.hasMultipleFilterSubjects config)
             ::headers-only-was-set? (.headersOnlyWasSet config)
             ::flow-control? (.isFlowControl config)
             ::headers-only? (.isHeadersOnly config)
             ::mem-storage? (.isMemStorage config)
             ::max-ack-pending-was-set? (.maxAckPendingWasSet config)
             ::max-batch-was-set? (.maxBatchWasSet config)
             ::max-bytes-was-set? (.maxBytesWasSet config)
             ::max-deliver-was-set? (.maxDeliverWasSet config)
             ::max-pull-waiting-was-set? (.maxPullWaitingWasSet config)
             ::mem-storage-was-set? (.memStorageWasSet config)
             ::metadata-was-set? (.metadataWasSet config)
             ::num-replicas-was-set? (.numReplicasWasSet config)
             ::rate-limit-was-set? (.rateLimitWasSet config)
             ::replay-policy-was-set? (.replayPolicyWasSet config)
             ::start-seq-was-set? (.startSeqWasSet config)}
      filter-subject (assoc ::filter-subject filter-subject))))

(defn ^:no-doc consumer-info->map [^ConsumerInfo info]
  {::ack-floor (some-> (.getAckFloor info) .getLastActive)
   ::calculated-pending (.getCalculatedPending info)
   ::cluster-info (cluster/cluster-info->map (.getClusterInfo info))
   ::consumer-configuration (consumer-configuration->map (.getConsumerConfiguration info))
   ::creation-time (some-> (.getCreationTime info) .toInstant)
   ::delivered (some-> (.getDelivered info) .getLastActive)
   ::name (.getName info)
   ::ack-pending (.getNumAckPending info)
   ::num-pending (.getNumPending info)
   ::num-waiting (.getNumWaiting info)
   ::paused (.getPaused info)
   ::pause-remaining (.getPauseRemaining info)
   ::redelivered (.getRedelivered info)
   ::stream-name (.getStreamName info)
   ::timestamp (some-> (.getTimestamp info) .toInstant)
   ::push-bound? (.isPushBound info)})

;; Build option classes

(defn ^:no-doc build-consumer-configuration
  [{::keys [ack-policy ack-wait backoff deliver-group deliver-policy deliver-subject
            description durable? filter-subjects flow-control headers-only?
            idle-heartbeat inactive-threshold max-ack-pending max-batch max-bytes
            max-deliver max-expires max-pull-waiting mem-storage? metadata
            num-replicas pause-until rate-limit replay-policy sample-frequency
            start-sequence start-time] :as opts}]
  (let [consumer-name (::name opts)]
    (assert (or (not durable?) (not (nil? consumer-name))) "Durable consumers must have a :nats.consumer/name")
    (cond-> ^ConsumerConfiguration$Builder (ConsumerConfiguration/builder)
      (ack-policies ack-policy) (.ackPolicy (ack-policies ack-policy))
      ack-wait (.ackWait ack-wait)
      backoff (.backoff backoff)
      deliver-group (.deliverGroup deliver-group)
      (deliver-policies deliver-policy) (.deliverPolicy (deliver-policies deliver-policy))
      deliver-subject (.deliverSubject deliver-subject)
      description (.description description)
      durable? (.durable consumer-name)
      filter-subjects (.filterSubjects (into-array String filter-subjects))
      flow-control (.flowControl flow-control)
      headers-only? (.headersOnly headers-only?)
      idle-heartbeat (.idleHeartbeat idle-heartbeat)
      inactive-threshold (.inactiveThreshold inactive-threshold)
      max-ack-pending (.maxAckPending max-ack-pending)
      max-batch (.maxBatch max-batch)
      max-bytes (.maxBytes max-bytes)
      max-deliver (.maxDeliver max-deliver)
      max-expires (.maxExpires max-expires)
      max-pull-waiting (.maxPullWaiting max-pull-waiting)
      mem-storage? (.memStorage mem-storage?)
      metadata (.metadata metadata)
      (and consumer-name (not durable?)) (.name consumer-name)
      num-replicas (.numReplicas num-replicas)
      pause-until (.pauseUntil pause-until)
      rate-limit (.rateLimit rate-limit)
      (replay-policies replay-policy) (.replayPolicy (replay-policies replay-policy))
      sample-frequency (.sampleFrequency sample-frequency)
      start-sequence (.startSequence start-sequence)
      start-time (.startTime (.atZone start-time nats/default-tz))
      :then (.build))))

(defn ^:no-doc build-consume-options [{:keys [batch-bytes batch-size bytes threshold-pct]}]
  (cond-> ^ConsumeOptions$Builder (ConsumeOptions/builder)
    batch-bytes (.batchBytes batch-bytes)
    batch-size (.batchSize batch-size)
    bytes (.bytes bytes)
    threshold-pct (.thresholdPercent threshold-pct)
    :then (.build)))

;; Public API

(defn ^{:style/indent 1 :export true} create-consumer
  "Create consumer. `config` is a map of:

   - `:nats.consumer/name`
   - `:nats.consumer/stream-name`
   - `:nats.consumer/ack-policy` - See `nats.consumer/ack-policies`
   - `:nats.consumer/ack-wait` - Number of milliseconds or a `java.time.Duration`
   - `:nats.consumer/deliver-policy` - See `nats.consumer/deliver-policies`
   - `:nats.consumer/description`
   - `:nats.consumer/durable?` - Makes stream durable.
   - `:nats.consumer/mem-storage?` - Forces consumer state to live in memory, instead of whatever the stream default is.
   - `:nats.consumer/filter-subjects`
   - `:nats.consumer/headers-only?`
   - `:nats.consumer/max-ack-pending` - Maximum outstanding acks before consumers are paused
   - `:nats.consumer/max-batch` - Maximum number of messages allowed in a single pull
   - `:nats.consumer/max-bytes` - Maximum number of bytes allowed in a single pull
   - `:nats.consumer/max-deliver` - Maximum number of times to deliver a message
   - `:nats.consumer/max-expires` - Number of milliseconds or a `java.time.Duration`
   - `:nats.consumer/max-pull-waiting` - Maximum number of outstanding pulls to accept
   - `:nats.consumer/metadata`
   - `:nats.consumer/num-replicas`
   - `:nats.consumer/replay-policy` - See `nats.consumer/replay-policies`
   - `:nats.consumer/sample-frequency` - Percentage of requests to sample for monitoring purposes
   - `:nats.consumer/start-sequence`
   - `:nats.consumer/sequence`
   - `:nats.consumer/start-time` - A `java.time.Instant`

  Options for ephemeral consumers:
   - `:nats.consumer/inactive-threshold` - Number of milliseconds or a `java.time.Duration`. Maximum idle time before consumer is removed.

  Options for push consumers (not recommended)
   - `:nats.consumer/flow-control` - Number of milliseconds or a `java.time.Duration`
   - `:nats.consumer/backoff`
   - `:nats.consumer/deliver-group`
   - `:nats.consumer/deliver-subject`
   - `:nats.consumer/idle-heartbeat` - Number of milliseconds or a `java.time.Duration`
   - `:nats.consumer/rate-limit`

  Requires a NATS 2.11 alpha server
   - `:nats.consumer/pause-until` - A `java.time.Instant`"
  [conn config]
  (->> (build-consumer-configuration config)
       (.addOrUpdateConsumer (stream/jet-stream-management conn) (::stream-name config))
       consumer-info->map))

(defn ^{:style/indent 1 :export true} update-consumer
  "Update consumer. See `create-consumer` for keys in `config`."
  [conn config]
  (create-consumer conn config))

(defn ^:export delete-consumer [conn stream-name consumer-name]
  (.deleteConsumer (stream/jet-stream-management conn) stream-name consumer-name))

(defn ^:export get-consumer-info [conn stream-name consumer-name]
  (-> (stream/jet-stream-management conn)
      (.getConsumerInfo stream-name consumer-name)
      consumer-info->map))

(defn ^:export get-consumer-names [conn stream-name]
  (set (.getConsumerNames (stream/jet-stream-management conn) stream-name)))

(defn ^:export get-consumers [conn stream-name]
  (set (map consumer-info->map (.getConsumers (stream/jet-stream-management conn) stream-name))))

;; NATS 2.11 features. Requires a preview version
(defn ^{:no-doc true :export true} pause-consumer [conn stream-name consumer-name ^Instant pause-until]
  (-> (stream/jet-stream-management conn)
      (.pauseConsumer stream-name consumer-name (.atZone pause-until nats/default-tz)))
  true)

;; NATS 2.11 features. Requires a preview version
(defn ^{:no-doc true :export true} resume-consumer [conn stream-name consumer-name]
  (-> (stream/jet-stream-management conn)
      (.resumeConsumer stream-name consumer-name)
      stream/stream-info->map))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to messages on `stream-name` for `consumer-name`. `opts` is a map of:

   - `:batch-bytes`
   - `:batch-size`
   - `:bytes`
   - `:threshold-pct`"
  [conn stream-name consumer-name & [opts]]
  (-> (.getStreamContext (nats/get-connection conn) stream-name)
      (.getConsumerContext consumer-name)
      (.iterate (build-consume-options opts))))

(defn ^:export pull-message [^IterableConsumer subscription timeout]
  (some-> (.nextMessage subscription timeout) message/message->map))

(defn ^:export unsubscribe [^IterableConsumer subscription]
  (.close subscription)
  nil)

(defn ^:export ack [conn message]
  (assert (not (nil? message)) "Can't ack without a message")
  (nats/publish conn
    {::message/subject (::message/reply-to message)
     ::message/data (.bodyBytes AckType/AckAck -1)}))

(defn ^:export nak [conn message]
  (assert (not (nil? message)) "Can't nak without a message")
  (nats/publish conn
    {::message/subject (::message/reply-to message)
     ::message/data (.bodyBytes AckType/AckNak -1)}))

(defn ^:export ack-in-progress [conn message]
  (assert (not (nil? message)) "Can't ack in progress without a message")
  (nats/publish conn
    {::message/subject (::message/reply-to message)
     ::message/data (.bodyBytes AckType/AckProgress -1)}))

(defn ^:export ack-term [conn message]
  (assert (not (nil? message)) "Can't ack term without a message")
  (nats/publish conn
    {::message/subject (::message/reply-to message)
     ::message/data (.bodyBytes AckType/AckTerm -1)}))
