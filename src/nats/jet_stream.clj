(ns nats.jet-stream
  (:require [nats.cluster :as cluster])
  (:import (io.nats.client.api CompressionOption ConsumerLimits DiscardPolicy External
                               Placement Republish RetentionPolicy SourceBase StorageType
                               StreamConfiguration StreamState Subject SubjectTransform)))

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

(defn ^{:style/indent 1 :export true} create-stream
  [conn {:keys [name
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
  (.addStream
   (.jetStreamManagement conn)
   (cond-> (StreamConfiguration/builder)
     name (.name name)
     description (.name description)
     subjects (.subjects (into-array String subjects))
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
     :always (.build))))

(defn ^:export get-cluster-info [conn stream-name]
  (-> (.jetStreamManagement conn)
      (.getStreamInfo stream-name)
      .getClusterInfo
      cluster/cluster-info->map))

(defn subject-transform->map [^SubjectTransform transform]
  {:destination (.getDestionation transform)
   :source (.getSource transform)})

(defn source-base->map [^SourceBase mirror]
  {:external (when-let [external ^External (.getExternal mirror)]
               {:api (.getApi external)
                :deliver (.getDeliver external)})
   :filter-subject (.getFilterSubject mirror)
   :name (.getName mirror)
   :source-name (.getSourceName mirror)
   :start-seq (.getStartSeq mirror)
   :start-time (.getStartTime mirror)
   :subject-transforms (map subject-transform->map (.getSubjectTransforms mirror))})

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
        sources (for [source (.getSources config)]
                  (source-base->map source))
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
             :subjects (seq (.getSubjects config))
             :discard-new-per-subject? (.isDiscardNewPerSubject config)}
      description (assoc :description description)
      mirror (assoc :mirror mirror)
      placement (assoc :placement placement)
      republish (assoc :republish republish)
      (seq sources) (assoc :sources sources)
      subject-transform (assoc :subject-transform subject-transform)
      template-owner (assoc :template-owner template-owner))))

(defn ^:export get-config [conn stream-name]
  (-> (.jetStreamManagement conn)
      (.getStreamInfo stream-name)
      .getConfiguration
      configuration->map))

(defn ^:export get-state [conn stream-name]
  (let [state ^StreamState (-> (.jetStreamManagement conn)
                               (.getStreamInfo stream-name)
                               .getStreamState)]
    {:byte-count (.getByteCount state)
     :consumer-count (.getConsumerCount state)
     :deleted (into [] (.getDeleted state))
     :deleted-count (.getDeletedCount state)
     :first-sequence-number (.getFirstSequence state)
     :first-time (.getFirstTime state)
     :last-time (.getLastTime state)
     :message-count (.getMsgCount state)
     :subject-count (.getSubjectCount state)
     :subjects (for [^Subject subject (.getSubjects state)]
                 {:count (.getCount subject)
                  :name (.getName subject)})}))
