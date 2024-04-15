(ns nats.kv
  (:require [nats.core :as nats]
            [nats.stream :as stream])
  (:import (io.nats.client JetStreamOptions JetStreamOptions$Builder
                           KeyValueOptions KeyValueOptions$Builder)
           (io.nats.client.api External External$Builder
                               KeyValueConfiguration KeyValueConfiguration$Builder
                               KeyValueStatus
                               Mirror Mirror$Builder
                               Placement Placement$Builder
                               Republish Republish$Builder
                               Source Source$Builder
                               SubjectTransform SubjectTransform$Builder)
           (java.time Duration)
           (java.util ArrayList)))

;; Map data classes to maps

(defn ^:no-doc key-value-status->map [^KeyValueStatus status]
  {})

;; Build options

(defn ^:no-doc build-jet-stream-options
  [{:nats.jso/keys [domain
                    opt-out-290-consumer-create?
                    prefix
                    publish-no-ack?
                    request-timeout]}]
  (cond-> ^JetStreamOptions$Builder (JetStreamOptions/builder)
    domain (.domain domain)
    opt-out-290-consumer-create? (.optOut290ConsumerCreate opt-out-290-consumer-create?)
    prefix (.prefix prefix)
    (boolean? publish-no-ack?) (.publishNoAck publish-no-ack?)
    request-timeout (.requestTimeout request-timeout)
    :then (.build)))

(defn ^:no-doc build-kvo-options [{::keys [stream-options domain prefix request-timeout]}]
  (cond-> ^KeyValueOptions$Builder (KeyValueOptions/builder)
    stream-options (.jetStreamOptions (build-jet-stream-options stream-options))
    domain (.jsDomain domain)
    prefix (.jsPrefix prefix)
    request-timeout (.jsRequestTimeout request-timeout)
    :then (.build)))

(defn ^:no-doc build-external-options [{:nats.external/keys [api deliver]}]
  (cond-> ^External$Builder (External/builder)
    api (.api api)
    deliver (.deliver deliver)
    :then (.build)))

(defn ^:no-doc build-subject-transform-options
  [{:nats.subject-transform/keys [destination source]}]
  (cond-> ^SubjectTransform$Builder (SubjectTransform/builder)
    destination (.destination destination)
    source (.source source)
    :then (.build)))

(defn ^:no-doc build-source-options
  [{:nats.source/keys [domain external filter-subject name source-name
                       start-seq start-time subject-transforms]}]
  (cond-> ^Source$Builder (Source/builder)
    domain (.domain domain)
    external (.external (build-external-options external))
    filter-subject (.filterSubject filter-subject)
    name (.name name)
    source-name (.sourceName source-name)
    start-seq (.startSeq start-seq)
    start-time (.startTime (.atZone start-time nats/default-tz))
    (seq subject-transforms) (.subjectTransforms (ArrayList. (map build-subject-transform-options subject-transforms)))
    :then (.build)))

(defn ^:no-doc build-mirror-options
  [{:nats.source/keys [domain external filter-subject name source-name
                       start-seq start-time subject-transforms]}]
  (cond-> ^Mirror$Builder (Mirror/builder)
    domain (.domain domain)
    external (.external (build-external-options external))
    filter-subject (.filterSubject filter-subject)
    name (.name name)
    source-name (.sourceName source-name)
    start-seq (.startSeq start-seq)
    start-time (.startTime (.atZone start-time nats/default-tz))
    (seq subject-transforms) (.subjectTransforms (ArrayList. (map build-subject-transform-options subject-transforms)))
    :then (.build)))

(defn ^:no-doc build-placement-options
  [{:nats.placement/keys [cluster tags]}]
  (cond-> ^Placement$Builder (Placement/builder)
    cluster (.cluster cluster)
    (seq tags) (.tags (ArrayList. (map name tags)))
    :then (.build)))

(defn ^:no-doc build-republish-options
  [{:nats.republish/keys [destination headers-only? source]}]
  (cond-> ^Republish$Builder (Republish/builder)
    destination (.destination destination)
    (boolean? headers-only?) (.headersOnly headers-only?)
    source (.source source)
    :then (.build)))

(defn ^:no-doc build-key-value-options
  [{::keys [sources
            compression?
            description
            max-bucket-size
            max-history-per-key
            max-value-size
            metadata
            mirror
            name
            placement
            replicas
            republish
            storage-type
            ttl]}]
  (cond-> ^KeyValueConfiguration$Builder (KeyValueConfiguration/builder)
    sources (.sources (map build-source-options sources))
    (boolean? compression?) (.compression compression?)
    description (.description description)
    max-bucket-size (.maxBucketSize max-bucket-size)
    max-history-per-key (.maxHistoryPerKey max-history-per-key)
    max-value-size (.maxValueSize max-value-size)
    metadata (.metadata (update-keys metadata clojure.core/name))
    mirror (.mirror (build-mirror-options mirror))
    name (.name name)
    placement (.placement (build-placement-options placement))
    replicas (.replicas replicas)
    republish (.republish (build-republish-options republish))
    storage-type (.storageType (stream/storage-types storage-type))
    ttl (.ttl (if (number? ttl)
                (Duration/ofMillis ttl)
                ttl))
    :then (.build)))

;; Helper function

(defn ^:no-doc kv-management [nats-conn]
  (let [{:keys [kvm conn key-value-options]} @nats-conn]
    (when-not kvm
      (->> (build-kvo-options key-value-options)
           (.keyValueManagement conn)
           (swap! nats-conn assoc :kvm))))
  (:kvm @nats-conn))

;; Public API

(defn ^{:style/indent 1 :export true} configure
  "Re-configure the KeyValue management instance. Returns a new `conn` with the
  new configuration, does not change the original `conn`.

  `key-value-options` is a map of:

  - `:nats.kv/stream-options` - JetStream configuration, see
    `nats.stream/configure` for details.
  - `:nats.kv/domain`
  - `:nats.kv/prefix`
  - `:nats.kv/request-timeout`"
  [conn key-value-options]
  (let [conn-val @conn]
    (-> (dissoc conn-val :kvo)
        (assoc :key-value-options key-value-options)
        atom)))

(defn ^{:style/indent 1 :export} create-bucket
  "Create a key/value bucket. `config` is a map of:

   - `:nats.kv/name`
   - `:nats.kv/description`
   - `:nats.kv/replicas`
   - `:nats.kv/storage-type` - See `nats.stream/storage-types`
   - `:nats.kv/ttl` - Number of milliseconds, or a `java.time.Duration`
   - `:nats.kv/compression?`
   - `:nats.kv/max-bucket-size`
   - `:nats.kv/max-history-per-key`
   - `:nats.kv/max-value-size`
   - `:nats.kv/metadata` - A map of {string|keyword string}
   - `:nats.kv/mirror` - A map, see below
   - `:nats.kv/placement` - A map, see below
   - `:nats.kv/republish` - A map, see below
   - `:nats.kv/sources` - A collection of sources, see below

   Keys in the `:nats.kv/mirror` map and entries in the `:nats.kv/sources` collection:
   - `:nats.source/domain`
   - `:nats.source/external`
   - `:nats.source/filter-subject`
   - `:nats.source/name`
   - `:nats.source/source-name`
   - `:nats.source/start-seq`
   - `:nats.source/start-time`
   - `:nats.source/subject-transforms`

   `:nats.kv/placement` map keys:
   - `:nats.placement/cluster`
   - `:nats.placement/tags`

   `:nats.kv/republish` map keys:
   - `:nats.republish/destination`
   - `:nats.republish/headers-only?`
   - `:nats.republish/source`"
  [conn config]
  (-> (kv-management conn)
      (.create (build-key-value-options config))
      key-value-status->map))

(defn ^:export update-bucket
  "Update a key/value bucket. See `create-bucket` for `config` details."
  [conn config]
  (-> (kv-management conn)
      (.update (build-key-value-options config))
      key-value-status->map))

(defn ^:export delete-bucket
  "Delete a key/value bucket"
  [conn bucket-name]
  (.delete (kv-management conn) bucket-name))

(defn ^:export get-bucket-status
  [conn bucket-name]
  (-> (kv-management conn)
      (.getStatus bucket-name)
      key-value-status->map))

(defn ^:export get-bucket-statuses [conn]
  (->> (.getStatuses (kv-management conn))
       (map key-value-status->map)
       set))
