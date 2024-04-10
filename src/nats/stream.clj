(ns nats.stream
  (:require [nats.cluster :as cluster]
            [nats.core :as nats])
  (:import (io.nats.client PurgeOptions PurgeOptions$Builder)
           (io.nats.client.api AccountLimits AccountStatistics AccountTier ConsumerConfiguration
                               ConsumerConfiguration$Builder ConsumerPauseResponse DeliverPolicy
                               ReplayPolicy AckPolicy ApiStats CompressionOption ConsumerLimits
                               DiscardPolicy External Placement Republish RetentionPolicy SourceBase
                               SourceInfoBase StorageType StreamConfiguration ConsumerInfo StreamInfo
                               StreamInfoOptions StreamInfoOptions$Builder StreamState Subject
                               SubjectTransform)
           (java.time ZonedDateTime)))

(def ack-policies
  {:nats.ack-policy/all AckPolicy/All
   :nats.ack-policy/explicit AckPolicy/Explicit
   :nats.ack-policy/none AckPolicy/None})

(def ack-policy->k
  (into {} (map (juxt second first) ack-policies)))

(def deliver-policies
  {:nats.deliver-policy/all DeliverPolicy/All
   :nats.deliver-policy/by-start-sequence DeliverPolicy/ByStartSequence
   :nats.deliver-policy/by-start-time DeliverPolicy/ByStartTime
   :nats.deliver-policy/last DeliverPolicy/Last
   :nats.deliver-policy/last-per-subject DeliverPolicy/LastPerSubject
   :nats.deliver-policy/new DeliverPolicy/New})

(def deliver-policy->k
  (into {} (map (juxt second first) deliver-policies)))

(def replay-policies
  {:nats.replay-policy/limits ReplayPolicy/Instant
   :nats.replay-policy/work-queue ReplayPolicy/Original})

(def replay-policy->k
  (into {} (map (juxt second first) replay-policies)))

(def retention-policies
  {:nats.retention-policy/limits RetentionPolicy/Limits
   :nats.retention-policy/work-queue RetentionPolicy/WorkQueue
   :nats.retention-policy/interest RetentionPolicy/Interest})

(def retention-policy->k
  (into {} (map (juxt second first) retention-policies)))

(def discard-policies
  {:nats.discard-policy/new DiscardPolicy/New
   :nats.discard-policy/old DiscardPolicy/Old})

(def discard-policy->k
  (into {} (map (juxt second first) discard-policies)))

(def compression-options
  {:nats.compression-option/none CompressionOption/None
   :nats.compression-option/s2 CompressionOption/S2})

(def compression-option->k
  (into {} (map (juxt second first) compression-options)))

(def storage-types
  {:nats.storage-type/file StorageType/File
   :nats.storage-type/memory StorageType/Memory})

(def storage-type->k
  (into {} (map (juxt second first) storage-types)))

(defn map->stream-configuration [{:keys [name
                                         description
                                         subjects
                                         retention-policy
                                         allow-direct-access?
                                         allow-rollup?
                                         deny-delete?
                                         deny-purge?
                                         max-age
                                         max-bytes
                                         max-consumers
                                         max-messages
                                         max-messages-per-subject
                                         max-msg-size
                                         replicas]}]
  (cond-> (StreamConfiguration/builder)
    name (.name name)
    description (.name description)
    subjects (.subjects (into-array String (map name subjects)))
    retention-policy (.retentionPolicy (retention-policies retention-policy))
    (boolean? allow-direct-access?) (.allowDirect allow-direct-access?)
    (boolean? allow-rollup?) (.allowRollup allow-rollup?)
    (boolean? deny-delete?) (.denyDelete deny-delete?)
    (boolean? deny-purge?) (.denyPurge deny-purge?)
    max-age (.maxAge max-age)
    max-bytes (.maxBytes max-bytes)
    max-consumers (.maxConsumers max-consumers)
    max-messages (.maxMessages max-messages)
    max-messages-per-subject (.maxMessagesPerSubject max-messages-per-subject)
    max-msg-size (.maxMsgSize max-msg-size)
    replicas (.replicas replicas)
    :always (.build)))

(defn ^:export get-stream-info-object [conn stream-name & [{:keys [include-deleted-details?
                                                                   filter-subjects]}]]
  (-> (.jetStreamManagement conn)
      (.getStreamInfo stream-name
                      (cond-> ^StreamInfoOptions$Builder (StreamInfoOptions/builder)
                        include-deleted-details? (.deletedDetails)
                        (seq filter-subjects) (.filterSubjects (map name filter-subjects))
                        :always (.build)))))

(defn ^:export get-cluster-info [conn stream-name & [options]]
  (some-> (get-stream-info-object conn stream-name options)
          .getClusterInfo
          cluster/cluster-info->map))

(defn subject-transform->map [^SubjectTransform transform]
  {:destination (.getDestionation transform)
   :source (.getSource transform)})

(defn external->map [^External external]
  {:api (.getApi external)
   :deliver (.getDeliver external)})

(defn source-base->map [^SourceBase mirror]
  (let [external (some-> (.getExternal mirror) external->map)]
    (cond-> {:filter-subject (.getFilterSubject mirror)
             :name (.getName mirror)
             :source-name (.getSourceName mirror)
             :start-seq (.getStartSeq mirror)
             :start-time (.getStartTime mirror)
             :subject-transforms (set (map subject-transform->map (.getSubjectTransforms mirror)))}
      external (assoc :external external))))

(defn consumer-limits->map [^ConsumerLimits consumer-limits]
  (let [inactive-threshold (.getInactiveThreshold consumer-limits)
        max-ack-pending (.getMaxAckPending consumer-limits)]
    (cond-> {}
      inactive-threshold (assoc :inactive-threshold inactive-threshold)
      max-ack-pending (assoc :max-ack-pending max-ack-pending))))

(defn placement->map [^Placement placement]
  {:cluster (.getCluster placement)
   :tags (seq (.getTags placement))})

(defn republish->map [^Republish republish]
  {:destination (.getDestionation republish)
   :source (.getSource republish)
   :headers-only? (.isHeadersOnly republish)})

(defn configuration->map [^StreamConfiguration config]
  (let [description (.getDescription config)
        mirror (some-> (.getMirror config) source-base->map)
        placement (some-> (.getPlacement config) placement->map)
        republish (some-> (.getRepublish config) republish->map)
        sources (set (for [source (.getSources config)]
                       (source-base->map source)))
        subject-transform (some-> (.getSubjectTransform config) subject-transform->map)
        template-owner (.getTemplateOwner config)]
    (cond-> {:allow-direct? (.getAllowDirect config)
             :allow-rollup? (.getAllowRollup config)
             :compression-option (compression-option->k (.getCompressionOption config))
             :consumer-limits (consumer-limits->map (.getConsumerLimits config))
             :deny-delete? (.getDenyDelete config)
             :deny-purge? (.getDenyPurge config)
             :discard-policy (discard-policy->k (.getDiscardPolicy config))
             :duplicate-window (.getDuplicateWindow config)
             :first-sequence (.getFirstSequence config)
             :max-age (.getMaxAge config)
             :maxBytes (.getMaxBytes config)
             :max-consumers (.getMaxConsumers config)
             :max-msgs (.getMaxMsgs config)
             :max-msg-size (.getMaxMsgSize config)
             :max-msgs-per-subject (.getMaxMsgsPerSubject config)
             :metadata (into {} (.getMetadata config))
             :mirror-direct? (.getMirrorDirect config)
             :name (.getName config)
             :no-ack? (.getNoAck config)
             :replicas (.getReplicas config)
             :retention-policy (retention-policy->k (.getRetentionPolicy config))
             :sealed? (.getSealed config)
             :storage-type (storage-type->k (.getStorageType config))
             :subjects (set (.getSubjects config))
             :discard-new-per-subject? (.isDiscardNewPerSubject config)}
      description (assoc :description description)
      mirror (assoc :mirror mirror)
      placement (assoc :placement placement)
      republish (assoc :republish republish)
      (seq sources) (assoc :sources sources)
      subject-transform (assoc :subject-transform subject-transform)
      template-owner (assoc :template-owner template-owner))))

(defn ^:export get-config [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getConfiguration
      configuration->map))

(defn source-info->map [^SourceInfoBase info]
  (let [error (.getError info)
        external (some-> (.getExternal info) external->map)
        subject-transforms (map subject-transform->map (.getSubjectTransforms info))]
    (cond-> {:active (.getActive info)
             :lag (.getLag info)
             :name (.getName info)}
      error (assoc :error error)
      external (assoc :external external)
      (seq subject-transforms) (assoc :subject-transforms subject-transforms))))

(defn ^:export get-mirror-info [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getMirrorInfo
      source-info->map))

(defn stream-state->map [^StreamState state]
  {:byte-count (.getByteCount state)
   :consumer-count (.getConsumerCount state)
   :deleted (into [] (.getDeleted state))
   :deleted-count (.getDeletedCount state)
   :first-sequence-number (.getFirstSequence state)
   :first-time (.getFirstTime state)
   :last-time (.getLastTime state)
   :message-count (.getMsgCount state)
   :subject-count (.getSubjectCount state)
   :subjects (set (for [^Subject subject (.getSubjects state)]
                    {:count (.getCount subject)
                     :name (.getName subject)}))})

(defn ^:export get-stream-state [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getStreamState
      stream-state->map))

(defn stream-info->map [^StreamInfo info]
  (let [source-infos (map source-info->map (.getSourceInfos info))
        cluster-info (.getClusterInfo info)
        mirror-info (.getMirrorInfo info)
        stream-state (.getStreamState info)]
    (cond-> {:create-time (.getCreateTime info)
             :configuration (configuration->map (.getConfiguration info))
             :timestamp (.getTimestamp info)}
      (seq source-infos) (assoc :source-infos source-infos)
      cluster-info (assoc :cluster-info (cluster/cluster-info->map cluster-info))
      mirror-info (assoc :mirror-info (source-info->map mirror-info))
      stream-state (assoc :stream-state (stream-state->map stream-state)))))

(defn ^:export get-stream-info [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      stream-info->map))

(defn ^:export get-stream-names [conn & [{:keys [subject-filter]}]]
  (if subject-filter
    (.getStreamNames (.jetStreamManagement conn) subject-filter)
    (.getStreamNames (.jetStreamManagement conn))))

(defn ^:export get-streams [conn & [{:keys [subject-filter]}]]
  (->> (if subject-filter
         (.getStreams (.jetStreamManagement conn) subject-filter)
         (.getStreams (.jetStreamManagement conn)))
       (map stream-info->map)))

(defn api-stats->map [^ApiStats api]
  {:errors (.getErrors api)
   :total (.getTotal api)})

(defn account-limits->map [^AccountLimits limits]
  {:max-ack-pending (.getMaxAckPending limits)
   :max-consumers (.getMaxConsumers limits)
   :max-memory (.getMaxMemory limits)
   :max-storage (.getMaxStorage limits)
   :max-streams (.getMaxStreams limits)
   :memory-max-stream-bytes (.getMemoryMaxStreamBytes limits)
   :storage-max-stream-bytes (.getStorageMaxStreamBytes limits)
   :max-bytes-required? (.isMaxBytesRequired limits)})

(defn account-tier->map [^AccountTier tier]
  {:limits (account-limits->map (.getLimits tier))
   :consumers (.getConsumers tier)
   :memory (.getMemory tier)
   :storage (.getStorage tier)
   :streams (.getStreams tier)})

(defn account-statistics->map [^AccountStatistics stats]
  {:api-stats (api-stats->map (.getApi stats))
   :consumers (.getConsumers stats)
   :limits (account-limits->map (.getLimits stats))
   :memory (.getMemory stats)
   :storage (.getStorage stats)
   :streams (.getStreams stats)
   :tiers (update-vals (.getTiers stats) account-tier->map)})

(defn ^:export get-account-statistics [conn]
  (-> (.jetStreamManagement conn)
      .getAccountStatistics
      account-statistics->map))

(defn ^:export get-first-message [conn stream-name subject]
  (-> (.jetStreamManagement conn)
      (.getFirstMessage stream-name (name subject))
      nats/message-info->map))

(defn ^:export get-last-message [conn stream-name subject]
  (-> (.jetStreamManagement conn)
      (.getLastMessage stream-name (name subject))
      nats/message-info->map))

(defn ^:export get-message [conn stream-name seq]
  (-> (.jetStreamManagement conn)
      (.getMessage stream-name seq)
      nats/message-info->map))

(defn ^:export get-next-message [conn stream-name seq subject]
  (-> (.jetStreamManagement conn)
      (.getNextMessage stream-name seq (name subject))
      nats/message-info->map))

(defn ^{:style/indent 1 :export true} create-stream
  "Adds a stream. See `map->stream-configuration` for valid options in `config`."
  [conn config]
  (-> (.jetStreamManagement conn)
      (.addStream (map->stream-configuration config))
      stream-info->map))

(defn ^{:style/indent 1 :export true} update-stream
  "Updates a stream. See `map->stream-configuration` for valid options in `config`."
  [conn config]
  (-> (.jetStreamManagement conn)
      (.updateStream (map->stream-configuration config))
      stream-info->map))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message to a JetStream subject. Performs publish acking if the stream
   requires it. Use `nats.core/publish` for regular PubSub messaging.

  message is a map of:

  - `:subject` - The subject to publish to
  - `:data` - The message data. Can be any Clojure value
  - `:headers` - An optional map of string keys to string (or collection of
                 string) values to set as meta-data on the message.
  - `:reply-to` - An optional reply-to subject."
  [conn message]
  (assert (not (nil? (:subject message))) "Can't publish without data")
  (assert (not (nil? (:data message))) "Can't publish nil data")
  (->> (nats/build-message message)
       (.publish (.jetStream conn))))

(defn build-consumer-configuration
  [{:keys [ack-policy ack-wait backoff deliver-group deliver-policy deliver-subject
           description durable filter-subject filter-subjects flow-control
           headers-only? idle-heartbeat inactive-threshold max-ack-pending max-batch
           max-bytes max-deliver max-expires max-pull-waiting mem-storage? metadata
           name num-replicas pause-until rate-limit replay-policy sample-frequency
           start-sequence sequence start-time]}]
  (cond-> ^ConsumerConfiguration$Builder (ConsumerConfiguration/builder)
    (ack-policies ack-policy) (.ackPolicy (ack-policies ack-policy))
    ack-wait (.ackWait ack-wait)
    backoff (.backoff backoff)
    deliver-group (.deliverGroup deliver-group)
    (deliver-policies deliver-policy) (.deliverPolicy (deliver-policies deliver-policy))
    deliver-subject (.deliverSubject (name deliver-subject))
    description (.description description)
    durable (.durable durable)
    filter-subject (.filterSubject (name filter-subject))
    filter-subjects (set (.filterSubjects (map name filter-subjects)))
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
    name (.name name)
    num-replicas (.numReplicas num-replicas)
    pause-until (.pauseUntil pause-until)
    rate-limit (.rateLimit rate-limit)
    (replay-policies replay-policy) (.replayPolicy (replay-policies replay-policy))
    sample-frequency (.sampleFrequency sample-frequency)
    start-sequence (.startSequence start-sequence)
    sequence (.startSequence sequence)
    start-time (.startTime start-time)
    :then (.build)))

(defn consumer-configuration->map [^ConsumerConfiguration config]
  {:ack-policy-was-set? (.ackPolicyWasSet config)
   :backoff-was-set? (.backoffWasSet config)
   :deliver-policy-was-set? (.deliverPolicyWasSet config)
   :flow-control-was-set? (.flowControlWasSet config)
   :ack-policy (ack-policy->k (.getAckPolicy config))
   :ack-wait (.getAckWait config)
   :backoff (seq (.getBackoff config))
   :deliver-group (.getDeliverGroup config)
   :deliver-policy (deliver-policy->k (.getDeliverPolicy config))
   :deliver-subject (.getDeliverSubject config)
   :description (.getDescription config)
   :durable (.getDurable config)
   :filter-subject (.getFilterSubject config)
   :filter-subjects (.getFilterSubjects config)
   :idle-heartbeat (.getIdleHeartbeat config)
   :inactve-threshold (.getInactiveThreshold config)
   :max-ack-pending (.getMaxAckPending config)
   :max-batch (.getMaxBatch config)
   :max-bytes (.getMaxBytes config)
   :max-deliver (.getMaxDeliver config)
   :max-expires (.getMaxExpires config)
   :max-pull-waiting (.getMaxPullWaiting config)
   :metadata (into {} (.getMetadata config))
   :name (.getName config)
   :num-replicas (.getNumReplicas config)
   :pause-until (.getPauseUntil config)
   :rate-limit (.getRateLimit config)
   :replay-policy (replay-policy->k (.getReplayPolicy config))
   :sample-frequency (.getSampleFrequency config)
   :start-sequence (.getStartSequence config)
   :start-time (.getStartTime config)
   :has-multiple-filter-subjects? (.hasMultipleFilterSubjects config)
   :headers-only-was-set? (.headersOnlyWasSet config)
   :flow-control? (.isFlowControl config)
   :headers-only? (.isHeadersOnly config)
   :mem-storage? (.isMemStorage config)
   :max-ack-pending-was-set? (.maxAckPendingWasSet config)
   :max-batch-was-set? (.maxBatchWasSet config)
   :max-bytes-was-set? (.maxBytesWasSet config)
   :max-deliver-was-set? (.maxDeliverWasSet config)
   :max-pull-waiting-was-set? (.maxPullWaitingWasSet config)
   :mem-storage-was-set? (.memStorageWasSet config)
   :metadata-was-set? (.metadataWasSet config)
   :num-replicas-was-set? (.numReplicasWasSet config)
   :rate-limit-was-set? (.rateLimitWasSet config)
   :replay-policy-was-set? (.replayPolicyWasSet config)
   :start-seq-was-set? (.startSeqWasSet config)})

(defn consumer-info->map [^ConsumerInfo info]
  {:ack-floor (some-> (.getAckFloor info) .getLastActive)
   :calculated-pending (.getCalculatedPending info)
   :cluster-info (cluster/cluster-info->map (.getClusterInfo info))
   :consumer-configuration (consumer-configuration->map (.getConsumerConfiguration info))
   :creation-time (.getCreationTime info)
   :delivered (some-> (.getDelivered info) .getLastActive)
   :name (.getName info)
   :ack-pending (.getNumAckPending info)
   :num-pending (.getNumPending info)
   :num-waiting (.getNumWaiting info)
   :paused (.getPaused info)
   :pause-remaining (.getPauseRemaining info)
   :redelivered (.getRedelivered info)
   :stream-name (.getStreamName info)
   :timestamp (.getTimestamp info)
   :push-bound? (.isPushBound info)})

(defn ^{:style/indent 1 :export true} add-consumer [conn stream-name configuration]
  (->> (build-consumer-configuration configuration)
       (.addOrUpdateConsumer (.jetStreamManagement conn) stream-name)
       consumer-info->map))

(defn ^{:style/indent 1 :export true} update-consumer [conn stream-name configuration]
  (add-consumer conn stream-name configuration))

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

(defn ^:export delete-message [conn stream-name seq-n & [{:keys [erase?] :as opt}]]
  (if opt
    (.deleteMessage (.jetStreamManagement conn) stream-name seq-n (boolean erase?))
    (.deleteMessage (.jetStreamManagement conn) stream-name seq-n)))

(defn ^:export delete-stream [conn stream-name]
  (.deleteStream (.jetStreamManagement conn) stream-name))

(defn build-purge-options [{:keys [keep sequence subject]}]
  (cond-> ^PurgeOptions$Builder (PurgeOptions/builder)
    keep (.keep keep)
    sequence (.sequence sequence)
    subject (.subject (name subject))))

(defn ^:export purge-stream [conn stream-name & [opts]]
  (-> (.jetStreamManagement conn)
      (.purgeStream stream-name (build-purge-options opts))))

(defn ^:export pause-consumer [conn stream-name consumer-name ^ZonedDateTime pause-until]
  (-> (.jetStreamManagement conn)
      (.pauseConsumer stream-name consumer-name pause-until))
  true)

(defn ^:export resume-consumer [conn stream-name consumer-name]
  (-> (.jetStreamManagement conn)
      (.resumeConsumer stream-name consumer-name)
      stream-info->map))
