(ns nats.stream
  (:require [clojure.set :as set]
            [nats.cluster :as cluster]
            [nats.core :as nats]
            [nats.message :as message])
  (:import (io.nats.client JetStream JetStreamOptions JetStreamOptions$Builder
                           Message PublishOptions PublishOptions$Builder
                           PurgeOptions PurgeOptions$Builder)
           (io.nats.client.api AccountLimits AccountStatistics AccountTier ApiStats
                               CompressionOption ConsumerLimits DiscardPolicy External
                               Placement Republish RetentionPolicy SourceBase SourceInfoBase
                               StorageType StreamConfiguration StreamInfo StreamInfoOptions
                               StreamInfoOptions$Builder StreamState Subject SubjectTransform)))

;; Enums as keywords

(def retention-policies
  "Available retention policies:

   - `:nats.retention-policy/limits`
   - `:nats.retention-policy/work-queue`
   - `:nats.retention-policy/interest`"
  {:nats.retention-policy/limits RetentionPolicy/Limits
   :nats.retention-policy/work-queue RetentionPolicy/WorkQueue
   :nats.retention-policy/interest RetentionPolicy/Interest})

(def ^:no-doc retention-policy->k (set/map-invert retention-policies))

(def discard-policies
  "Available discard policies:

   - `:nats.discard-policy/new`
   - `:nats.discard-policy/old`"
  {:nats.discard-policy/new DiscardPolicy/New
   :nats.discard-policy/old DiscardPolicy/Old})

(def ^:no-doc discard-policy->k (set/map-invert discard-policies))

(def compression-options
  "Available compression options

   - `:nats.compression-option/none`
   - `:nats.compression-option/s2`"
  {:nats.compression-option/none CompressionOption/None
   :nats.compression-option/s2 CompressionOption/S2})

(def ^:no-doc compression-option->k (set/map-invert compression-options))

(def storage-types
  "Available storage types

   - `:nats.storage-type/file`
   - `:nats.storage-type/memory`"
  {:nats.storage-type/file StorageType/File
   :nats.storage-type/memory StorageType/Memory})

(def ^:no-doc storage-type->k (set/map-invert storage-types))

;; Map data classes to maps

(defn ^:no-doc subject-transform->map [^SubjectTransform transform]
  {:nats.subject-transform/destination (.getDestionation transform)
   :nats.subject-transform/source (.getSource transform)})

(defn ^:no-doc external->map [^External external]
  {:nats.external/api (.getApi external)
   :nats.external/deliver (.getDeliver external)})

(defn ^:no-doc source-base->map [^SourceBase mirror]
  (let [external (some-> (.getExternal mirror) external->map)]
    (cond-> {:nats.source/filter-subject (.getFilterSubject mirror)
             :nats.source/name (.getName mirror)
             :nats.source/source-name (.getSourceName mirror)
             :nats.source/start-seq (.getStartSeq mirror)
             :nats.source/start-time (some-> (.getStartTime mirror) .toInstant)
             :nats.source/subject-transforms (set (map subject-transform->map (.getSubjectTransforms mirror)))}
      external (assoc :nats.source/external external))))

(defn ^:no-doc consumer-limits->map [^ConsumerLimits consumer-limits]
  (let [inactive-threshold (.getInactiveThreshold consumer-limits)
        max-ack-pending (.getMaxAckPending consumer-limits)]
    (cond-> {}
      inactive-threshold (assoc :nats.limits/inactive-threshold inactive-threshold)
      max-ack-pending (assoc :nats.limits/max-ack-pending max-ack-pending))))

(defn ^:no-doc placement->map [^Placement placement]
  {:nats.placement/cluster (.getCluster placement)
   :nats.placement/tags (seq (.getTags placement))})

(defn ^:no-doc republish->map [^Republish republish]
  {:nats.publish/destination (.getDestionation republish)
   :nats.publish/source (.getSource republish)
   :nats.publish/headers-only? (.isHeadersOnly republish)})

(defn ^:no-doc configuration->map [^StreamConfiguration config]
  (let [description (.getDescription config)
        mirror (some-> (.getMirror config) source-base->map)
        placement (some-> (.getPlacement config) placement->map)
        republish (some-> (.getRepublish config) republish->map)
        sources (set (for [source (.getSources config)]
                       (source-base->map source)))
        subject-transform (some-> (.getSubjectTransform config) subject-transform->map)
        template-owner (.getTemplateOwner config)]
    (cond-> {::allow-direct? (.getAllowDirect config)
             ::allow-rollup? (.getAllowRollup config)
             ::compression-option (compression-option->k (.getCompressionOption config))
             ::consumer-limits (consumer-limits->map (.getConsumerLimits config))
             ::deny-delete? (.getDenyDelete config)
             ::deny-purge? (.getDenyPurge config)
             ::discard-policy (discard-policy->k (.getDiscardPolicy config))
             ::duplicate-window (.getDuplicateWindow config)
             ::first-sequence (.getFirstSequence config)
             ::max-age (.getMaxAge config)
             ::max-bytes (.getMaxBytes config)
             ::max-consumers (.getMaxConsumers config)
             ::max-msgs (.getMaxMsgs config)
             ::max-msg-size (.getMaxMsgSize config)
             ::max-msgs-per-subject (.getMaxMsgsPerSubject config)
             ::metadata (into {} (.getMetadata config))
             ::mirror-direct? (.getMirrorDirect config)
             ::name (.getName config)
             ::no-ack? (.getNoAck config)
             ::replicas (.getReplicas config)
             ::retention-policy (retention-policy->k (.getRetentionPolicy config))
             ::sealed? (.getSealed config)
             ::storage-type (storage-type->k (.getStorageType config))
             ::subjects (set (.getSubjects config))
             ::discard-new-per-subject? (.isDiscardNewPerSubject config)}
      description (assoc ::description description)
      mirror (assoc ::mirror mirror)
      placement (assoc ::placement placement)
      republish (assoc ::republish republish)
      (seq sources) (assoc ::sources sources)
      subject-transform (assoc ::subject-transform subject-transform)
      template-owner (assoc ::template-owner template-owner))))

(defn ^:no-doc source-info->map [^SourceInfoBase info]
  (let [error (.getError info)
        external (some-> (.getExternal info) external->map)
        subject-transforms (map subject-transform->map (.getSubjectTransforms info))]
    (cond-> {:nats.source/active (.getActive info)
             :nats.source/lag (.getLag info)
             :nats.source/name (.getName info)}
      error (assoc :nats.source/error error)
      external (assoc :nats.source/external external)
      (seq subject-transforms) (assoc :nats.source/subject-transforms subject-transforms))))

(defn ^:no-doc stream-state->map [^StreamState state]
  {::byte-count (.getByteCount state)
   ::consumer-count (.getConsumerCount state)
   ::deleted (into #{} (.getDeleted state))
   ::deleted-count (.getDeletedCount state)
   ::first-sequence-number (.getFirstSequence state)
   ::first-time (some-> (.getFirstTime state) .toInstant)
   ::last-time (some-> (.getLastTime state) .toInstant)
   ::message-count (.getMsgCount state)
   ::subject-count (.getSubjectCount state)
   ::subjects
   (set (for [^Subject subject (.getSubjects state)]
          {:nats.subject/count (.getCount subject)
           :nats.subject/name (.getName subject)}))})

(defn ^:no-doc stream-info->map [^StreamInfo info]
  (let [source-infos (map source-info->map (.getSourceInfos info))
        cluster-info (.getClusterInfo info)
        mirror-info (.getMirrorInfo info)
        stream-state (.getStreamState info)]
    (cond-> {::create-time (some-> (.getCreateTime info) .toInstant)
             ::configuration (configuration->map (.getConfiguration info))
             ::timestamp (some-> (.getTimestamp info) .toInstant)}
      (seq source-infos) (assoc ::source-infos source-infos)
      cluster-info (assoc ::cluster-info (cluster/cluster-info->map cluster-info))
      mirror-info (assoc ::mirror-info (source-info->map mirror-info))
      stream-state (assoc ::stream-state (stream-state->map stream-state)))))

(defn ^:no-doc api-stats->map [^ApiStats api]
  {:errors (.getErrors api)
   :total (.getTotal api)})

(defn ^:no-doc account-limits->map [^AccountLimits limits]
  {:nats.account.limits/max-ack-pending (.getMaxAckPending limits)
   :nats.account.limits/max-consumers (.getMaxConsumers limits)
   :nats.account.limits/max-memory (.getMaxMemory limits)
   :nats.account.limits/max-storage (.getMaxStorage limits)
   :nats.account.limits/max-streams (.getMaxStreams limits)
   :nats.account.limits/memory-max-stream-bytes (.getMemoryMaxStreamBytes limits)
   :nats.account.limits/storage-max-stream-bytes (.getStorageMaxStreamBytes limits)
   :nats.account.limits/max-bytes-required? (.isMaxBytesRequired limits)})

(defn ^:no-doc account-tier->map [^AccountTier tier]
  {:nats.account.tier/limits (account-limits->map (.getLimits tier))
   :nats.account.tier/consumers (.getConsumers tier)
   :nats.account.tier/memory (.getMemory tier)
   :nats.account.tier/storage (.getStorage tier)
   :nats.account.tier/streams (.getStreams tier)})

(defn ^:no-doc account-statistics->map [^AccountStatistics stats]
  (let [tiers (.getTiers stats)]
    (cond-> {:nats.account/api-stats (api-stats->map (.getApi stats))
             :nats.account/consumers (.getConsumers stats)
             :nats.account/limits (account-limits->map (.getLimits stats))
             :nats.account/memory (.getMemory stats)
             :nats.account/storage (.getStorage stats)
             :nats.account/streams (.getStreams stats)}
      (not-empty tiers)
      (assoc :nats.account/tiers (update-vals tiers account-tier->map)))))

;; Build option classes

(defn ^:no-doc build-stream-configuration
  [{::keys [description
            subjects
            retention-policy
            allow-direct?
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
    (::name opts) (.name (::name opts))
    description (.description description)
    subjects (.subjects (into-array String subjects))
    retention-policy (.retentionPolicy (retention-policies retention-policy))
    (boolean? allow-direct?) (.allowDirect allow-direct?)
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

(defn ^:no-doc build-publish-options
  [{:nats.publish/keys [expected-last-msg-id
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
    expected-stream (.expectedStream expected-stream)
    message-id (.messageId message-id)
    stream (.stream stream)
    stream-timeout (.streamTimeout stream-timeout)
    :then (.build)))

(defn ^:no-doc build-purge-options [{:keys [keep sequence subject]}]
  (cond-> ^PurgeOptions$Builder (PurgeOptions/builder)
    keep (.keep keep)
    sequence (.sequence sequence)
    subject (.subject subject)
    :then (.build)))

(defn ^:no-doc build-jet-stream-options
  [{::keys [domain
            opt-out-290-consumer-create?
            prefix
            publish-no-ack?
            request-timeout]}]
  (cond-> ^JetStreamOptions$Builder (JetStreamOptions/builder)
    domain (.domain domain)
    (boolean? opt-out-290-consumer-create?) (.optOut290ConsumerCreate opt-out-290-consumer-create?)
    prefix (.prefix prefix)
    (boolean? publish-no-ack?) (.publishNoAck publish-no-ack?)
    request-timeout (.requestTimeout request-timeout)
    :then (.build)))

;; Helper functions

(defn ^:no-doc jet-stream-management [nats-conn]
  (let [{:keys [jsm conn jet-stream-options]} @nats-conn]
    (when-not jsm
      (->> (build-jet-stream-options jet-stream-options)
           (.jetStreamManagement conn)
           (swap! nats-conn assoc :jsm))))
  (:jsm @nats-conn))

(defn ^:no-doc get-stream-info-object
  [conn stream-name & [{:keys [include-deleted-details?
                               filter-subjects]}]]
  (-> (jet-stream-management conn)
      (.getStreamInfo stream-name
                      (cond-> ^StreamInfoOptions$Builder (StreamInfoOptions/builder)
                        include-deleted-details? (.deletedDetails)
                        (seq filter-subjects) (.filterSubjects filter-subjects)
                        :always (.build)))))

;; Public API

(defn ^:export configure
  "Re-configure the JetStream management instance. Returns a new `conn` with the
  new configuration, does not change the original `conn`.

  `jet-stream-options` is a map of:

  - `:nats.stream/domain`
  - `:nats.stream/opt-out-290-consumer-create?`
  - `:nats.stream/prefix`
  - `:nats.stream/publish-no-ack?`
  - `:nats.stream/request-timeout`"
  [conn jet-stream-options]
  (let [conn-val @conn]
    (-> (dissoc conn-val :jsm)
        (assoc :jet-stream-options jet-stream-options)
        atom)))

(defn ^:export get-cluster-info [conn stream-name & [options]]
  (some-> (get-stream-info-object conn stream-name options)
          .getClusterInfo
          cluster/cluster-info->map))

(defn ^:export get-stream-config
  "Get the configuration for `stream-name`. `opts` is a map of:

  - `:include-deleted-details?`
  - `:filter-subjects`"
  [conn stream-name & [opts]]
  (-> (get-stream-info-object conn stream-name opts)
      .getConfiguration
      configuration->map))

(defn ^:export get-mirror-info
  "Get the mirror info for `stream-name`. `opts` is a map of:

  - `:include-deleted-details?`
  - `:filter-subjects`"
  [conn stream-name & [opts]]
  (some-> (get-stream-info-object conn stream-name opts)
          .getMirrorInfo
          source-info->map))

(defn ^:export get-stream-state
  "Get the state for `stream-name`. `opts` is a map of:

  - `:include-deleted-details?`
  - `:filter-subjects`"
  [conn stream-name & [opts]]
  (-> (get-stream-info-object conn stream-name opts)
      .getStreamState
      stream-state->map))

(defn ^:export get-stream-info
  "Get the information about `stream-name`. `opts` is a map of:

  - `:include-deleted-details?`
  - `:filter-subjects`"
  [conn stream-name & [opts]]
  (-> (get-stream-info-object conn stream-name opts)
      stream-info->map))

(defn ^:export get-stream-names [conn & [{:keys [subject-filter]}]]
  (set
   (if subject-filter
     (.getStreamNames (jet-stream-management conn) subject-filter)
     (.getStreamNames (jet-stream-management conn)))))

(defn ^:export get-streams [conn & [{:keys [subject-filter]}]]
  (set (->> (if subject-filter
              (.getStreams (jet-stream-management conn) subject-filter)
              (.getStreams (jet-stream-management conn)))
            (map stream-info->map))))

(defn ^:export get-account-statistics [conn]
  (-> (jet-stream-management conn)
      .getAccountStatistics
      account-statistics->map))

(defn ^:export get-first-message [conn stream-name subject]
  (-> (jet-stream-management conn)
      (.getFirstMessage stream-name subject)
      message/message-info->map))

(defn ^:export get-last-message [conn stream-name subject]
  (-> (jet-stream-management conn)
      (.getLastMessage stream-name subject)
      message/message-info->map))

(defn ^:export get-message [conn stream-name seq]
  (-> (jet-stream-management conn)
      (.getMessage stream-name seq)
      message/message-info->map))

(defn ^:export get-next-message [conn stream-name seq subject]
  (-> (jet-stream-management conn)
      (.getNextMessage stream-name seq subject)
      message/message-info->map))

(defn ^{:style/indent 1 :export true} create-stream
  "Adds a stream. `config` is a map of the following keys:

   - :nats.stream/name
   - :nats.stream/description
   - :nats.stream/subjects
   - :nats.stream/retention-policy
   - :nats.stream/allow-direct?
   - :nats.stream/allow-rollup?
   - :nats.stream/deny-delete?
   - :nats.stream/deny-purge?
   - :nats.stream/max-age
   - :nats.stream/max-bytes
   - :nats.stream/max-consumers
   - :nats.stream/max-messages
   - :nats.stream/max-messages-per-subject
   - :nats.stream/max-msg-size
   - :nats.stream/replicas"
  [conn config]
  (-> (jet-stream-management conn)
      (.addStream (build-stream-configuration config))
      stream-info->map))

(defn ^{:style/indent 1 :export true} update-stream
  "Updates a stream. See `create-stream` for valid options in `config`."
  [conn config]
  (-> (jet-stream-management conn)
      (.updateStream (build-stream-configuration config))
      stream-info->map))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message to a JetStream subject. Performs publish acking if the stream
   requires it. Use `nats.core/publish` for regular PubSub messaging.

  `message` is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.
  - `:nats.message/reply-to` - An optional reply-to subject.

  `opts` is a map of:

  - `:expected-last-msg-id`
  - `:expected-last-sequence`
  - `:expected-last-subject-sequence`
  - `:expected-stream`
  - `:message-id`
  - `:stream`
  - `:stream-timeout`"
  [conn message & [opts]]
  (assert (not (nil? (::message/subject message))) "Can't publish without data")
  (assert (not (nil? (::message/data message))) "Can't publish nil data")
  (try
    (->> ^PublishOptions (build-publish-options opts)
         (.publish ^JetStream (.jetStream (nats/get-connection conn)) ^Message (message/build-message message))
         message/publish-ack->map)
    (catch java.io.IOException e
      (if (empty? (get-streams conn {:subject-filter (:nats.message/subject message)}))
        (throw (ex-info "nats.stream/publish published to a non-stream subject, and failed to ack. If you meant to publish to a stream, check the subject, else use nats.core/publish"
                        {:message message} e))
        (throw e)))))

(defn ^:export delete-message
  "Delete the specific message from `stream-name` with `seq-n`."
  [conn stream-name seq-n & [{:keys [erase?] :as opt}]]
  (if opt
    (.deleteMessage (jet-stream-management conn) stream-name seq-n (boolean erase?))
    (.deleteMessage (jet-stream-management conn) stream-name seq-n)))

(defn ^:export delete-stream [conn stream-name]
  (.deleteStream (jet-stream-management conn) stream-name))

(defn ^:export purge-stream
  "Purge stream `stream-name`. `opts` is a map of:

  - `:keep`
  - `:sequence`
  - `:subject`"
  [conn stream-name & [opts]]
  (-> (jet-stream-management conn)
      (.purgeStream stream-name (build-purge-options opts))))
