(ns nats.stream
  (:require [clojure.set :as set]
            [nats.cluster :as cluster]
            [nats.core :as nats]
            [nats.message :as message])
  (:import (io.nats.client JetStream Message PublishOptions PublishOptions$Builder
                           PurgeOptions PurgeOptions$Builder)
           (io.nats.client.api AccountLimits AccountStatistics AccountTier ApiStats
                               CompressionOption ConsumerLimits DiscardPolicy External
                               Placement Republish RetentionPolicy SourceBase SourceInfoBase
                               StorageType StreamConfiguration StreamInfo StreamInfoOptions
                               StreamInfoOptions$Builder StreamState Subject SubjectTransform)))

(def retention-policies
  {:nats.retention-policy/limits RetentionPolicy/Limits
   :nats.retention-policy/work-queue RetentionPolicy/WorkQueue
   :nats.retention-policy/interest RetentionPolicy/Interest})

(def retention-policy->k (set/map-invert retention-policies))

(def discard-policies
  {:nats.discard-policy/new DiscardPolicy/New
   :nats.discard-policy/old DiscardPolicy/Old})

(def discard-policy->k (set/map-invert discard-policies))

(def compression-options
  {:nats.compression-option/none CompressionOption/None
   :nats.compression-option/s2 CompressionOption/S2})

(def compression-option->k (set/map-invert compression-options))

(def storage-types
  {:nats.storage-type/file StorageType/File
   :nats.storage-type/memory StorageType/Memory})

(def storage-type->k (set/map-invert storage-types))

(defn build-stream-configuration
  [{::keys [description
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
            replicas] :as opts}]
  (cond-> (StreamConfiguration/builder)
    (::name opts) (.name (name (::name opts)))
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

(defn get-stream-info-object
  [conn stream-name & [{:keys [include-deleted-details?
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
  {:nats.subject-transform/destination (.getDestionation transform)
   :nats.subject-transform/source (.getSource transform)})

(defn external->map [^External external]
  {:nats.external/api (.getApi external)
   :nats.external/deliver (.getDeliver external)})

(defn source-base->map [^SourceBase mirror]
  (let [external (some-> (.getExternal mirror) external->map)]
    (cond-> {:nats.source/filter-subject (.getFilterSubject mirror)
             :nats.source/name (.getName mirror)
             :nats.source/source-name (.getSourceName mirror)
             :nats.source/start-seq (.getStartSeq mirror)
             :nats.source/start-time (some-> (.getStartTime mirror) .toInstant)
             :nats.source/subject-transforms (set (map subject-transform->map (.getSubjectTransforms mirror)))}
      external (assoc :nats.source/external external))))

(defn consumer-limits->map [^ConsumerLimits consumer-limits]
  (let [inactive-threshold (.getInactiveThreshold consumer-limits)
        max-ack-pending (.getMaxAckPending consumer-limits)]
    (cond-> {}
      inactive-threshold (assoc :nats.limits/inactive-threshold inactive-threshold)
      max-ack-pending (assoc :nats.limits/max-ack-pending max-ack-pending))))

(defn placement->map [^Placement placement]
  {:nats.placement/cluster (.getCluster placement)
   :nats.placement/tags (seq (.getTags placement))})

(defn republish->map [^Republish republish]
  {:nats.publish/destination (.getDestionation republish)
   :nats.publish/source (.getSource republish)
   :nats.publish/headers-only? (.isHeadersOnly republish)})

(defn configuration->map [^StreamConfiguration config]
  (let [description (.getDescription config)
        mirror (some-> (.getMirror config) source-base->map)
        placement (some-> (.getPlacement config) placement->map)
        republish (some-> (.getRepublish config) republish->map)
        sources (set (for [source (.getSources config)]
                       (source-base->map source)))
        subject-transform (some-> (.getSubjectTransform config) subject-transform->map)
        template-owner (.getTemplateOwner config)]
    (cond-> {:nats.stream/allow-direct? (.getAllowDirect config)
             :nats.stream/allow-rollup? (.getAllowRollup config)
             :nats.stream/compression-option (compression-option->k (.getCompressionOption config))
             :nats.stream/consumer-limits (consumer-limits->map (.getConsumerLimits config))
             :nats.stream/deny-delete? (.getDenyDelete config)
             :nats.stream/deny-purge? (.getDenyPurge config)
             :nats.stream/discard-policy (discard-policy->k (.getDiscardPolicy config))
             :nats.stream/duplicate-window (.getDuplicateWindow config)
             :nats.stream/first-sequence (.getFirstSequence config)
             :nats.stream/max-age (.getMaxAge config)
             :nats.stream/maxBytes (.getMaxBytes config)
             :nats.stream/max-consumers (.getMaxConsumers config)
             :nats.stream/max-msgs (.getMaxMsgs config)
             :nats.stream/max-msg-size (.getMaxMsgSize config)
             :nats.stream/max-msgs-per-subject (.getMaxMsgsPerSubject config)
             :nats.stream/metadata (into {} (.getMetadata config))
             :nats.stream/mirror-direct? (.getMirrorDirect config)
             :nats.stream/name (.getName config)
             :nats.stream/no-ack? (.getNoAck config)
             :nats.stream/replicas (.getReplicas config)
             :nats.stream/retention-policy (retention-policy->k (.getRetentionPolicy config))
             :nats.stream/sealed? (.getSealed config)
             :nats.stream/storage-type (storage-type->k (.getStorageType config))
             :nats.stream/subjects (set (.getSubjects config))
             :nats.stream/discard-new-per-subject? (.isDiscardNewPerSubject config)}
      description (assoc :nats.stream/description description)
      mirror (assoc :nats.stream/mirror mirror)
      placement (assoc :nats.stream/placement placement)
      republish (assoc :nats.stream/republish republish)
      (seq sources) (assoc :nats.stream/sources sources)
      subject-transform (assoc :nats.stream/subject-transform subject-transform)
      template-owner (assoc :nats.stream/template-owner template-owner))))

(defn ^:export get-stream-config [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getConfiguration
      configuration->map))

(defn source-info->map [^SourceInfoBase info]
  (let [error (.getError info)
        external (some-> (.getExternal info) external->map)
        subject-transforms (map subject-transform->map (.getSubjectTransforms info))]
    (cond-> {:nats.source/active (.getActive info)
             :nats.source/lag (.getLag info)
             :nats.source/name (.getName info)}
      error (assoc :nats.source/error error)
      external (assoc :nats.source/external external)
      (seq subject-transforms) (assoc :nats.source/subject-transforms subject-transforms))))

(defn ^:export get-mirror-info [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getMirrorInfo
      source-info->map))

(defn stream-state->map [^StreamState state]
  {:nats.stream/byte-count (.getByteCount state)
   :nats.stream/consumer-count (.getConsumerCount state)
   :nats.stream/deleted (into [] (.getDeleted state))
   :nats.stream/deleted-count (.getDeletedCount state)
   :nats.stream/first-sequence-number (.getFirstSequence state)
   :nats.stream/first-time (some-> (.getFirstTime state) .toInstant)
   :nats.stream/last-time (some-> (.getLastTime state) .toInstant)
   :nats.stream/message-count (.getMsgCount state)
   :nats.stream/subject-count (.getSubjectCount state)
   :nats.stream/subjects
   (set (for [^Subject subject (.getSubjects state)]
          {:nats.subject/count (.getCount subject)
           :nats.subject/name (.getName subject)}))})

(defn ^:export get-stream-state [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      .getStreamState
      stream-state->map))

(defn stream-info->map [^StreamInfo info]
  (let [source-infos (map source-info->map (.getSourceInfos info))
        cluster-info (.getClusterInfo info)
        mirror-info (.getMirrorInfo info)
        stream-state (.getStreamState info)]
    (cond-> {:create-time (some-> (.getCreateTime info) .toInstant)
             :configuration (configuration->map (.getConfiguration info))
             ::nats/timestamp (some-> (.getTimestamp info) .toInstant)}
      (seq source-infos) (assoc :source-infos source-infos)
      cluster-info (assoc :cluster-info (cluster/cluster-info->map cluster-info))
      mirror-info (assoc :mirror-info (source-info->map mirror-info))
      stream-state (assoc :stream-state (stream-state->map stream-state)))))

(defn ^:export get-stream-info [conn stream-name & [options]]
  (-> (get-stream-info-object conn stream-name options)
      stream-info->map))

(defn get-stream-names [conn & [{:keys [subject-filter]}]]
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
      message/message-info->map))

(defn ^:export get-last-message [conn stream-name subject]
  (-> (.jetStreamManagement conn)
      (.getLastMessage stream-name (name subject))
      message/message-info->map))

(defn ^:export get-message [conn stream-name seq]
  (-> (.jetStreamManagement conn)
      (.getMessage stream-name seq)
      message/message-info->map))

(defn ^:export get-next-message [conn stream-name seq subject]
  (-> (.jetStreamManagement conn)
      (.getNextMessage stream-name seq (name subject))
      message/message-info->map))

(defn ^{:style/indent 1 :export true} create-stream
  "Adds a stream. See `build-stream-configuration` for valid options in `config`."
  [conn config]
  (-> (.jetStreamManagement conn)
      (.addStream (build-stream-configuration config))
      stream-info->map))

(defn ^{:style/indent 1 :export true} update-stream
  "Updates a stream. See `build-stream-configuration` for valid options in `config`."
  [conn config]
  (-> (.jetStreamManagement conn)
      (.updateStream (build-stream-configuration config))
      stream-info->map))

(defn build-publish-options [{:keys [expected-last-msg-id
                                     expected-last-sequence
                                     expected-last-subject-sequence
                                     expected-stream
                                     message-id
                                     stream
                                     stream-timeout]}]
  (cond-> ^PublishOptions$Builder (PublishOptions/builder)
    expected-last-msg-id (.expectedLastMsgId expected-last-msg-id)
    expected-last-sequence (.expectedLastSequence expected-last-sequence)
    expected-last-subject-sequence (.expectedLastSubjectSequence expected-last-subject-sequence)
    expected-stream (.expectedStream (name expected-stream))
    message-id (.messageId message-id)
    stream (.stream (name stream))
    stream-timeout (.streamTimeout stream-timeout)
    :then (.build)))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message to a JetStream subject. Performs publish acking if the stream
   requires it. Use `nats.core/publish` for regular PubSub messaging.

  message is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.
  - `:nats.message/reply-to` - An optional reply-to subject.

  See `build-publish-options` for details on `opts`."
  [conn message & [opts]]
  (assert (not (nil? (::message/subject message))) "Can't publish without data")
  (assert (not (nil? (::message/data message))) "Can't publish nil data")
  (->> ^PublishOptions (build-publish-options opts)
       (.publish ^JetStream (.jetStream conn) ^Message (message/build-message message))
       message/publish-ack->map))

(defn ^:export delete-message [conn stream-name seq-n & [{:keys [erase?] :as opt}]]
  (if opt
    (.deleteMessage (.jetStreamManagement conn) stream-name seq-n (boolean erase?))
    (.deleteMessage (.jetStreamManagement conn) stream-name seq-n)))

(defn ^:export delete-stream [conn stream-name]
  (.deleteStream (.jetStreamManagement conn) stream-name))

(defn build-purge-options [{::keys [keep sequence subject]}]
  (cond-> ^PurgeOptions$Builder (PurgeOptions/builder)
    keep (.keep keep)
    sequence (.sequence sequence)
    subject (.subject (name subject))))

(defn ^:export purge-stream [conn stream-name & [opts]]
  (-> (.jetStreamManagement conn)
      (.purgeStream stream-name (build-purge-options opts))))
