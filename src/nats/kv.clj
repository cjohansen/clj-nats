(ns nats.kv
  (:require [clojure.set :as set]
            [nats.core :as nats]
            [nats.message :as message]
            [nats.stream :as stream])
  (:refer-clojure :exclude [get])
  (:import (io.nats.client KeyValueOptions KeyValueOptions$Builder
                           Message)
           (io.nats.client.api External External$Builder
                               KeyValueConfiguration KeyValueConfiguration$Builder
                               KeyValueEntry KeyValueOperation KeyValueStatus
                               KeyValuePurgeOptions KeyValuePurgeOptions$Builder
                               KeyValueWatcher KeyValueWatchOption
                               MessageInfo Mirror Mirror$Builder
                               Placement Placement$Builder
                               Republish Republish$Builder
                               Source Source$Builder
                               SubjectTransform SubjectTransform$Builder)
           (io.nats.client.impl CljNatsKeyValue)
           (io.nats.client.support NatsKeyValueUtil)
           (java.time Duration)
           (java.util ArrayList)))

;; Enums

(def operations
  "Available key value operations:

   - `:nats.kv-operation/delete`
   - `:nats.kv-operation/purge`
   - `:nats.kv-operation/put`"
  {:nats.kv-operation/delete KeyValueOperation/DELETE
   :nats.kv-operation/purge KeyValueOperation/PURGE
   :nats.kv-operation/put KeyValueOperation/PUT})

(def ^:no-doc operation->k (set/map-invert operations))

(def watch-options
  {:nats.kv.watch-option/ignore-delete KeyValueWatchOption/IGNORE_DELETE
   :nats.kv.watch-option/include-history KeyValueWatchOption/INCLUDE_HISTORY
   :nats.kv.watch-option/meta-only KeyValueWatchOption/META_ONLY
   :nats.kv.watch-option/updates-only KeyValueWatchOption/UPDATES_ONLY})

;; Interfaces

(definterface PrefixedConsumer
  ;; This allows us to override the getConsumerNamePrefix in KeyValueWatcher.
  ;; Because it is implemented with a default method,
  ;; Clojure cannot see it as part of the interface.
  (^String getConsumerNamePrefix []))

;; Map data classes to maps

(defn ^:no-doc key-value-configuration->map [^KeyValueConfiguration config]
  (let [mirror (some-> (.getMirror config) stream/source-base->map)
        republish (some-> (.getRepublish config) stream/republish->map)
        sources (set (for [source (.getSources config)]
                       (stream/source-base->map source)))]
    (cond-> {::max-history-per-key (.getMaxHistoryPerKey config)
             ::max-value-size (.getMaxValueSize config)}
      mirror (assoc ::mirror mirror)
      republish (assoc ::republish republish)
      (seq sources) (assoc ::sources sources))))

(defn ^:no-doc key-value-status->map [^KeyValueStatus status]
  (let [metadata (some->> (.getMetadata status) (into {}))
        placement (some-> (.getPlacement status) stream/placement->map)
        republish (some-> (.getRepublish status) stream/republish->map)]
    (cond-> {::backing-store (.getBackingStore status)
             ::stream-info (stream/stream-info->map (.getBackingStreamInfo status))
             ::bucket-name (.getBucketName status)
             ::byte-count (.getByteCount status)
             ::configuration (key-value-configuration->map (.getConfiguration status))
             ::description (.getDescription status)
             ::entry-count (.getEntryCount status)
             ::max-bucket-size (.getMaxBucketSize status)
             ::max-history-per-key (.getMaxHistoryPerKey status)
             ::max-value-size (.getMaxValueSize status)
             ::replicas (.getReplicas status)
             ::storage-type (stream/storage-type->k (.getStorageType status))
             ::ttl (.getTtl status)
             ::compressed? (.isCompressed status)}
      metadata (assoc ::metadata metadata)
      placement (assoc ::placement placement)
      republish (assoc ::republish republish))))

(defn message-info->key-value-entry [bucket k ^MessageInfo msg]
  (let [headers (.getHeaders msg)]
    {:nats.kv.entry/bucket bucket
     :nats.kv.entry/key k
     :nats.kv.entry/created-at (.toInstant (.getTime msg))
     :nats.kv.entry/operation (operation->k (NatsKeyValueUtil/getOperation headers))
     :nats.kv.entry/revision (.getSeq msg)
     :nats.kv.entry/value (message/get-message-data (message/headers->map headers) (.getData msg))}))

(defn message->key-value-entry [bucket k ^Message msg]
  (let [headers (.getHeaders msg)]
    {:nats.kv.entry/bucket bucket
     :nats.kv.entry/key k
     :nats.kv.entry/created-at (.toInstant (.timestamp (.metaData msg)))
     :nats.kv.entry/operation (operation->k (NatsKeyValueUtil/getOperation headers))
     :nats.kv.entry/revision (.streamSequence (.metaData msg))
     :nats.kv.entry/value (message/get-message-data (message/headers->map headers) (.getData msg))}))

(defn native->key-value-entry [^KeyValueEntry entry]
  {:nats.kv.entry/bucket (.getBucket entry)
   :nats.kv.entry/key (.getKey entry)
   :nats.kv.entry/created-at (.toInstant (.getCreated entry))
   :nats.kv.entry/operation (operation->k (.getOperation entry))
   :nats.kv.entry/revision (.getRevision entry)
   :nats.kv.entry/value (.getValueAsString entry)})

;; Build options

(defn ^:no-doc build-kvo-options [{::keys [stream-options domain prefix request-timeout]}]
  (cond-> ^KeyValueOptions$Builder (KeyValueOptions/builder)
    stream-options (.jetStreamOptions (stream/build-jet-stream-options stream-options))
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
            bucket-name
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
    bucket-name (.name bucket-name)
    placement (.placement (build-placement-options placement))
    replicas (.replicas replicas)
    republish (.republish (build-republish-options republish))
    storage-type (.storageType (stream/storage-types storage-type))
    ttl (.ttl (if (number? ttl)
                (Duration/ofMillis ttl)
                ttl))
    :then (.build)))

;; Helper functions

(defn ^:no-doc bucket-management [nats-conn]
  (let [{:keys [kvbm conn key-value-options]} @nats-conn]
    (when-not kvbm
      (->> (build-kvo-options key-value-options)
           (.keyValueManagement conn)
           (swap! nats-conn assoc :kvbm))))
  (:kvbm @nats-conn))

(defn ^:no-doc kv-management [nats-conn bucket-name]
  (let [{:keys [kvm conn key-value-options]} @nats-conn]
    (when-not (get-in kvm [bucket-name])
      (->> (build-kvo-options key-value-options)
           (CljNatsKeyValue/create conn bucket-name)
           (swap! nats-conn assoc-in [:kvm bucket-name]))))
  (get-in @nats-conn [:kvm bucket-name]))

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
    (-> (dissoc conn-val :kvo :kvbm)
        (assoc :key-value-options key-value-options)
        atom)))

(defn ^{:style/indent 1 :export true} create-bucket
  "Create a key/value bucket. `config` is a map of:

   - `:nats.kv/bucket-name`
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
  (-> (bucket-management conn)
      (.create (build-key-value-options config))
      key-value-status->map))

(defn ^:export get-bucket-status
  [conn bucket-name]
  (-> (bucket-management conn)
      (.getStatus bucket-name)
      key-value-status->map))

(defn ^{:style/indent 1 :export true} update-bucket
  "Update a key/value bucket. See `create-bucket` for `config` details."
  [conn config]
  (let [config (merge (get-bucket-status conn (::bucket-name config)) config)]
    (-> (bucket-management conn)
        (.update (build-key-value-options config))
        key-value-status->map)))

(defn ^:export delete-bucket
  "Delete a key/value bucket"
  [conn bucket-name]
  (.delete (bucket-management conn) bucket-name))

(defn ^:export get-bucket-statuses [conn]
  (->> (.getStatuses (bucket-management conn))
       (map key-value-status->map)
       set))

(defn- write-key [conn bucket-name k v headers]
  (let [{:keys [kind data]} (message/get-message-body v)
        all-headers (into (message/set-content-type nil kind) headers)]
    (-> (kv-management conn bucket-name)
        (.put k data (some-> all-headers message/map->Headers)))))

(defn ^:export put
  "Put a key in the bucket. `v` can be either a byte array or any serializable
  Clojure value.

  Supports two arities:

  ```
  (put conn :bucket/key v)
  (put conn \"bucket\" \"key\" v)
  ```"
  ([conn k v]
   (write-key conn (namespace k) (name k) v nil))
  ([conn bucket-name k v]
   (write-key conn bucket-name k v nil)))

(defn ^:export get
  "Get the entry for key `k`. Returns the version specified by `rev`, or the
  current one.

  ```
  (get conn :bucket/key)
  (get conn :bucket/key 42)
  (get conn \"bucket\" \"key\" 42)
  ```"
  ([conn k]
   (get conn (namespace k) (name k) nil))
  ([conn k rev]
   (get conn (namespace k) (name k) rev))
  ([conn bucket-name k rev]
   (when-let [message-info (if rev
                             (.getMessage (kv-management conn bucket-name) k rev)
                             (.getMessage (kv-management conn bucket-name) k))]
     (let [entry (message-info->key-value-entry bucket-name k message-info)]
       (when (= :nats.kv-operation/put (:nats.kv.entry/operation entry))
         entry)))))

(defn ^:export get-value
  "Get the value for key `k`. Returns the version specified by `rev`, or the
  current one.

  ```
  (get-value conn :bucket/key)
  (get-value conn :bucket/key 42)
  (get-value conn \"bucket\" \"key\" 42)
  ```"
  ([conn k]
   (get-value conn (namespace k) (name k) nil))
  ([conn k rev]
   (get-value conn (namespace k) (name k) rev))
  ([conn bucket-name k rev]
   (:nats.kv.entry/value (get conn bucket-name k rev))))

(defn ^:export cas
  "Compare and swap. Sets the value of `k` to `v`, only if the current revision is
  still `rev`.

  Returns the new revision number for the key.

  ```
  (cas conn :bucket/key \"val\" 32)
  (cas conn \"bucket\" \"key\" \"val\" 32)
  ```"
  ([conn k v rev]
   (cas conn (namespace k) (name k) v rev))
  ([conn bucket-name k v rev]
   (write-key conn bucket-name k v {"Nats-Expected-Last-Subject-Sequence" (str rev)})))

(defn ^:export delete
  "Delete the value at key `k`. When providing the optional `expected-rev`, only
  delete the value if the current revision is still `expected-rev`.

  Returns the new revision number for the key.

  ```
  (delete conn :bucket/key)

  ;; Delete specific revision
  (delete conn :bucket/key 32)

  (delete conn \"bucket\" \"key\" 32)

  ;; Explicitly name bucket and key without specifying revision
  (delete conn \"bucket\" \"key\" nil)
  ```"
  ([conn k]
   (delete conn (namespace k) (name k) nil))
  ([conn k expected-rev]
   (delete conn (namespace k) (name k) expected-rev))
  ([conn bucket-name k expected-rev]
   (let [headers (cond-> {"KV-Operation" "DEL"}
                   expected-rev (assoc "Nats-Expected-Last-Subject-Sequence" (str expected-rev)))]
     (write-key conn bucket-name k nil headers))))

(defn ^:export get-history
  "Returns all revisions of the key `k`.

  ```
  (get-history conn :bucket/key)
  (get-history conn \"bucket\" \"key\")
  ```"
  ([conn k]
   (get-history conn (namespace k) (name k)))
  ([conn bucket-name k]
   (->> (.getHistory (kv-management conn bucket-name) k)
        (map #(message->key-value-entry bucket-name k %)))))

(defn ^:export get-keys
  "Return a set of all the keys in the bucket as strings"
  [conn bucket-name]
  (set (.keys (kv-management conn bucket-name))))

(defn ^:export purge
  "Purge all history for a single key, optionally specify the expected current revision.

  ```
  (purge conn :bucket/key)
  (purge conn :bucket/key 23)
  (purge conn \"bucket\" \"key\" 23)
  ```"
  ([conn k]
   (purge conn (namespace k) (name k) nil))
  ([conn k expected-rev]
   (purge conn (namespace k) (name k) expected-rev))
  ([conn bucket-name k expected-rev]
   (if expected-rev
     (.purge (kv-management conn bucket-name) k expected-rev)
     (.purge (kv-management conn bucket-name) k))))

(defn ^:export purge-deleted
  "Purges history for deleted keys in `bucket-name`"
  [conn bucket-name & [{:keys [delete-marker-threshold]}]]
  (.purgeDeletes
   (kv-management conn bucket-name)
   (cond-> ^KeyValuePurgeOptions$Builder (KeyValuePurgeOptions/builder)
     delete-marker-threshold (.deleteMarkersThreshold delete-marker-threshold)
     :then .build)))

(defn ^:export watch
  ([conn bucket-name watcher]
   (watch conn bucket-name nil nil watcher nil))
  ([conn bucket-name watcher watch-opts]
   (watch conn bucket-name nil nil watcher watch-opts))
  ([conn bucket-name subjects watcher watch-opts]
   (watch conn bucket-name subjects nil watcher watch-opts))
  ([conn bucket-name subjects from-rev {:keys [watch end-of-data consumer-name-prefix]} watch-opts]
   (let [subjects (cond
                    (nil? subjects) [">"]
                    (string? subjects) [subjects]
                    (instance? java.util.List subjects) subjects
                    (seqable? subjects) (vec subjects)
                    :else subjects)]
     (.watch (kv-management conn bucket-name)
             ^java.util.List subjects
             (reify
               KeyValueWatcher
               (watch [_ e] (watch (native->key-value-entry e)))
               (endOfData [_] (when end-of-data (end-of-data)))

               PrefixedConsumer
               (getConsumerNamePrefix [_] consumer-name-prefix))
             (or from-rev -1)
             (into-array KeyValueWatchOption (map watch-options watch-opts))))))
