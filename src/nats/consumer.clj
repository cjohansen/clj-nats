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

(def ack-policies
  {:nats.ack-policy/all AckPolicy/All
   :nats.ack-policy/explicit AckPolicy/Explicit
   :nats.ack-policy/none AckPolicy/None})

(def ack-policy->k (set/map-invert ack-policies))

(def deliver-policies
  {:nats.deliver-policy/all DeliverPolicy/All
   :nats.deliver-policy/by-start-sequence DeliverPolicy/ByStartSequence
   :nats.deliver-policy/by-start-time DeliverPolicy/ByStartTime
   :nats.deliver-policy/last DeliverPolicy/Last
   :nats.deliver-policy/last-per-subject DeliverPolicy/LastPerSubject
   :nats.deliver-policy/new DeliverPolicy/New})

(def deliver-policy->k (set/map-invert deliver-policies))

(def replay-policies
  {:nats.replay-policy/limits ReplayPolicy/Instant
   :nats.replay-policy/work-queue ReplayPolicy/Original})

(def replay-policy->k (set/map-invert replay-policies))

(defn build-consumer-configuration
  [{::keys [ack-policy ack-wait backoff deliver-group deliver-policy deliver-subject
            description durable? filter-subject filter-subjects flow-control
            headers-only? idle-heartbeat inactive-threshold max-ack-pending max-batch
            max-bytes max-deliver max-expires max-pull-waiting mem-storage? metadata
            num-replicas pause-until rate-limit replay-policy sample-frequency
            start-sequence sequence start-time] :as opts}]
  (let [consumer-name (::name opts)]
    (assert (or (not durable?) (not (nil? consumer-name))) "Durable consumers must have a :nats.consumer/name")
    (cond-> ^ConsumerConfiguration$Builder (ConsumerConfiguration/builder)
      (ack-policies ack-policy) (.ackPolicy (ack-policies ack-policy))
      ack-wait (.ackWait ack-wait)
      backoff (.backoff backoff)
      deliver-group (.deliverGroup deliver-group)
      (deliver-policies deliver-policy) (.deliverPolicy (deliver-policies deliver-policy))
      deliver-subject (.deliverSubject (name deliver-subject))
      description (.description description)
      durable? (.durable consumer-name)
      filter-subject (.filterSubject (name filter-subject))
      filter-subjects (.filterSubjects (map name filter-subjects))
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
      sequence (.startSequence sequence)
      start-time (.startTime (.atZone start-time nats/default-tz))
      :then (.build))))

(defn consumer-configuration->map [^ConsumerConfiguration config]
  {::ack-policy-was-set? (.ackPolicyWasSet config)
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
   ::filter-subject (.getFilterSubject config)
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
   ::start-seq-was-set? (.startSeqWasSet config)})

(defn consumer-info->map [^ConsumerInfo info]
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

(defn ^{:style/indent 1 :export true} create-consumer [conn configuration]
  (->> (build-consumer-configuration configuration)
       (.addOrUpdateConsumer (.jetStreamManagement conn) (::stream-name configuration))
       consumer-info->map))

(defn ^{:style/indent 1 :export true} update-consumer [conn configuration]
  (create-consumer conn configuration))

(defn ^:export delete-consumer [conn stream-name consumer-name]
  (.deleteConsumer (.jetStreamManagement conn) stream-name consumer-name))

(defn ^:export get-consumer-info [conn stream-name consumer-name]
  (-> (.jetStreamManagement conn)
      (.getConsumerInfo stream-name consumer-name)
      consumer-info->map))

(defn ^:export get-consumer-names [conn stream-name]
  (.getConsumerNames (.jetStreamManagement conn) stream-name))

(defn ^:export get-consumers [conn stream-name]
  (map consumer-info->map (.getConsumers (.jetStreamManagement conn) stream-name)))

(defn ^:export pause-consumer [conn stream-name consumer-name ^Instant pause-until]
  (-> (.jetStreamManagement conn)
      (.pauseConsumer stream-name consumer-name (.atZone pause-until nats/default-tz)))
  true)

(defn ^:export resume-consumer [conn stream-name consumer-name]
  (-> (.jetStreamManagement conn)
      (.resumeConsumer stream-name consumer-name)
      stream/stream-info->map))
(defn build-consume-options [{:keys [batch-bytes batch-size bytes threshold-pct]}]
  (cond-> ^ConsumeOptions$Builder (ConsumeOptions/builder)
    batch-bytes (.batchBytes batch-bytes)
    batch-size (.batchSize batch-size)
    bytes (.bytes bytes)
    threshold-pct (.thresholdPercent threshold-pct)
    :then (.build)))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to messages on `stream-name` for `consumer-name`. Refer to
  `build-consume-options` for keys in `opts`."
  [conn stream-name consumer-name & [opts]]
  (-> (.getStreamContext conn stream-name)
      (.getConsumerContext consumer-name)
      (.iterate (build-consume-options opts))))

(defn ^:export pull-message [^IterableConsumer subscription timeout]
  (some-> (.nextMessage subscription timeout) message/message->map))

(defn ^:export unsubscribe [^IterableConsumer subscription]
  (.close subscription)
  nil)

(defn ^:export ack [conn message]
  (nats/publish conn {::nats/subject (:reply-to message)
                      ::message/data (.bodyBytes AckType/AckAck -1)}))

(defn ^:export nak [conn message]
  (nats/publish conn {::nats/subject (:reply-to message)
                      ::message/data (.bodyBytes AckType/AckNak -1)}))

(defn ^:export ack-in-progress [conn message]
  (nats/publish conn {::nats/subject (:reply-to message)
                      ::message/data (.bodyBytes AckType/AckProgress -1)}))

(defn ^:export ack-term [conn message]
  (nats/publish conn {::nats/subject (:reply-to message)
                      ::message/data (.bodyBytes AckType/AckTerm -1)}))
