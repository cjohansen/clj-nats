(ns nats.object-store
  "Object Store lets you store, list and watch binary large objects (BLOBs)"
  (:refer-clojure :exclude [list])
  (:import (io.nats.client Connection ObjectStore ObjectStoreManagement ObjectStoreOptions ObjectStoreOptions$Builder)
           (io.nats.client.api ObjectInfo ObjectInfo$Builder ObjectStoreConfiguration ObjectStoreConfiguration$Builder
                               ObjectMeta ObjectMetaOptions ObjectLink
                               ObjectStoreStatus ObjectStoreWatcher ObjectStoreWatchOption
                               Placement StorageType)
           (io.nats.client.impl NatsObjectStoreWatchSubscription)
           (java.io ByteArrayOutputStream)
           (java.time Duration ZonedDateTime)
           (java.util Map))
  (:require [clojure.spec.alpha :as s]
            [nats.message :as message]
            [nats.object :as-alias object]
            [nats.stream :as stream]))

;; Map data classes to maps

(defn ^:no-doc object-store-status->map [^ObjectStoreStatus status]
  (let [metadata (some->> (.getMetadata status) (into {}))
        placement (some-> (.getPlacement status) stream/placement->map)]
    (cond-> {::backing-store (.getBackingStore status)
             ::stream-info (stream/stream-info->map (.getBackingStreamInfo status))
             ::bucket-name (.getBucketName status)
             ::description (.getDescription status)
             ::max-bucket-size (.getMaxBucketSize status)
             ::replicas (.getReplicas status)
             ::total-byte-size (.getSize status)
             ::storage-type (stream/storage-type->k (.getStorageType status))
             ::ttl (.getTtl status)
             ::compressed? (.isCompressed status)
             ::sealed? (.isSealed status)}
      metadata (assoc ::metadata metadata)
      placement (assoc ::placement placement))))

;; Build options

(defn ^:no-doc build-object-store-options [{::keys [stream-options]}]
  (-> (ObjectStoreOptions/builder (stream/build-jet-stream-options stream-options))
      (ObjectStoreOptions$Builder/.build)))

(defn ^:no-doc build-object-store-configuration
  [{::keys [^String bucket-name
            ^boolean compression?
            ^String description
            ^long max-bucket-size
            ^Map metadata
            ^Placement placement
            ^int replicas
            ^StorageType storage-type
            ^Duration ttl]}]
  (cond-> ^ObjectStoreConfiguration$Builder (ObjectStoreConfiguration/builder)
    bucket-name (.name bucket-name)
    compression? (.compression compression?)
    description (.description description)
    max-bucket-size (.maxBucketSize max-bucket-size)
    metadata (.metadata ^Map (update-keys metadata clojure.core/name))
    placement (.placement placement)
    replicas (.replicas replicas)
    storage-type (.storageType storage-type)
    ttl (.ttl ttl)
    :then .build))

;; Helper functions

(defn ^:no-doc bucket-management [nats-conn]
  (let [{:keys [osbm conn object-store-options]} @nats-conn]
    (when-not osbm
      (->> (build-object-store-options object-store-options)
           (Connection/.objectStoreManagement conn)
           (swap! nats-conn assoc :osbm))))
  (:osbm @nats-conn))

(defn ^:no-doc object-store-management [nats-conn bucket-name]
  (let [{:keys [osm conn object-store-options]} @nats-conn]
    (when-not (get-in osm [bucket-name])
      (->> (build-object-store-options object-store-options)
           (Connection/.objectStore conn bucket-name)
           (swap! nats-conn assoc-in [:osm bucket-name]))))
  (get-in @nats-conn [:osm bucket-name]))

;; Public API

(defn ^{:style/indent 1 :export true} configure
  "Re-configure the ObjectStore management instance. Returns a new `conn` with the
  new configuration, does not change the original `conn`.

  `object-store-options` is a map of:

  - `:nats.object-store/stream-options` - JetStream configuration, see
    `nats.stream/configure` for details.
  - `:nats.object-store/domain`
  - `:nats.object-store/prefix`
  - `:nats.object-store/request-timeout`"
  [conn object-store-options]
  (let [conn-val @conn]
    (-> (dissoc conn-val :osm :osbm)
        (assoc :object-store-options object-store-options)
        atom)))

(defn ^{:style/indent 1 :export true} create-bucket
  [conn config]
  (-> (bucket-management conn)
      (ObjectStoreManagement/.create (build-object-store-configuration config))
      object-store-status->map))

(defn ^:export get-bucket-status
  [conn bucket-name]
  (-> (bucket-management conn)
      (ObjectStoreManagement/.getStatus bucket-name)
      object-store-status->map))

(defn ^:export delete-bucket
  "Delete a key/value bucket"
  [conn bucket-name]
  (ObjectStoreManagement/.delete (bucket-management conn) bucket-name))

(defn ^:export get-bucket-statuses [conn]
  (into #{}
        (map object-store-status->map)
        (ObjectStoreManagement/.getStatuses (bucket-management conn))))

(s/def :nats.object/bucket string?)
(s/def :nats.object/description string?)
(s/def :nats.object/deleted? boolean?)
(s/def :nats.object/name string?)
(s/def :nats.object/digest string?)
(s/def :nats.object/modified-zdt #(instance? ZonedDateTime %))
(s/def :nats.object/size-bytes number?)
(s/def :nats.object/headers (s/map-of string? string?))
(s/def :nats.object/nuid string?)

(def object-info-accessors
  {:nats.object/bucket {:accessor ObjectInfo/.getBucket}
   :nats.object/chunks {:accessor ObjectInfo/.getChunks}
   :nats.object/deleted? {:accessor ObjectInfo/.isDeleted}
   :nats.object/description {:accessor ObjectInfo/.getDescription}
   :nats.object/digest {:accessor ObjectInfo/.getDigest}
   :nats.object/headers {:accessor ObjectInfo/.getHeaders
                         :parser #'nats.message/headers->map}
   :nats.object/modified-zdt {:accessor ObjectInfo/.getModified}
   :nats.object/name {:accessor ObjectInfo/.getObjectName}
   :nats.object/nuid {:accessor ObjectInfo/.getNuid}
   :nats.object/size-bytes {:accessor ObjectInfo/.getSize}
   ;; TODO model io.nats.client.api.ObjectLink
   })

(defn ObjectInfo->map [^ObjectInfo object-info]
  (reduce (fn [m [k {:keys [accessor parser]}]]
            (let [value (accessor object-info)]
              (if (some? value)
                (assoc m k (cond-> value
                             parser parser))
                m)))
          {}
          object-info-accessors))

(defn put-bytes [conn bucket ^String object-name ^bytes bytes]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)]
    (-> (ObjectStore/.put object-store object-name bytes)
        ObjectInfo->map)))

(defn put-str [conn bucket ^String object-name ^String s]
  (put-bytes conn bucket object-name (String/.getBytes s "UTF-8")))

(defn get-bytes ^bytes [conn bucket ^String object-name]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)
        buffer (ByteArrayOutputStream/new)]
    (ObjectStore/.get object-store object-name buffer)
    (ByteArrayOutputStream/.toByteArray buffer)))

(defn get-str [conn bucket ^String object-name]
  (String. (get-bytes conn bucket object-name) "UTF-8"))

(defn list
  "List object information for all objects in bucket"
  [conn bucket]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)]
    (map ObjectInfo->map (ObjectStore/.getList object-store))))

(defn get-info
  "Get information about an object, without transferring the object itself

  Pass :nats.object/include-deleted? to get information about deleted objects.
  Returns nil if no object was found. "
  [conn bucket ^String object-name & {::object/keys [include-deleted?]}]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)]
    (some-> (if include-deleted?
              (ObjectStore/.getInfo object-store object-name true)
              (ObjectStore/.getInfo object-store object-name))
            ObjectInfo->map)))

(defn delete
  "Delete object given its name

  Idempotent except for :nats.object/modified-zdt, which may change if delete is
  called more times."
  [conn bucket ^String object-name]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)]
    (some-> (ObjectStore/.delete object-store object-name) ObjectInfo->map)))

(defn seal!
  "Prohibit future mutation of this object store

  Return status for this bucket. Idempotent except for :nats.stream/timestamp
  for the backing stream. "
  [conn bucket]
  (-> (ObjectStore/.seal (Connection/.objectStore (:conn @conn) bucket))
      object-store-status->map))

(defn watch
  "Watch an object store for changes.

  Optionally also iterate over the changes that happened before the watcher was
  created. An object store watcher will create a NATS consumer.

  - `on-change` is called for each change to the bucket with a map of
    information about the changed object.
  - `on-end-of-data` (optional) is called when all historical events untill the
    present have been consumed.
  - `get-consumer-name-prefix` (optional) sets the prefix for the NATS consumer
    backing the watcher.

  watch returns a subscription, which must be closed to avoid resource leaks.
  Either use a with-open block,

    (with-open [_ (object-store/watch conn store)]
      ,,,)

  , or call object-store/unwatch when you're done with it."
  ^NatsObjectStoreWatchSubscription
  [conn bucket on-change & {:keys [on-end-of-data get-consumer-name-prefix watch-options]}]
  (let [watcher (if get-consumer-name-prefix
                  (reify ObjectStoreWatcher
                    (watch [_this object-info]
                      (on-change (ObjectInfo->map object-info)))
                    (endOfData [_this]
                      (when on-end-of-data
                        (on-end-of-data)))
                    (getConsumerNamePrefix [_this]
                      (get-consumer-name-prefix)))
                  (reify ObjectStoreWatcher
                    (watch [_ object-info]
                      (on-change (ObjectInfo->map object-info)))
                    (endOfData [_this]
                      (when on-end-of-data
                        (on-end-of-data)))))]
    (ObjectStore/.watch (Connection/.objectStore (:conn @conn) bucket)
                        watcher
                        ;; TODO qualified kewords for watch options, pass a set
                        (make-array ObjectStoreWatchOption 0))))

(defn unwatch [subscription]
  (NatsObjectStoreWatchSubscription/.close subscription))

(defn resolve-link
  "Resolve links to buckets or objects

  Returns a map of :nats.object/bucket and :nats.object/bucket when the link
  target is an object, a map of :nats.object/bucket when the link target is a bucket,
  nil otherwise."
  [conn bucket link-name]
  (when-let [objectLink
             (some-> (Connection/.objectStore (:conn @conn) bucket)
                     (ObjectStore/.getInfo link-name)
                     ObjectInfo/.getObjectMeta
                     ObjectMeta/.getObjectMetaOptions
                     ObjectMetaOptions/.getLink)]
    (cond-> {:nats.object/bucket (ObjectLink/.getBucket objectLink)}
      (ObjectLink/.getObjectName objectLink)
      (assoc :nats.object/name (ObjectLink/.getObjectName objectLink)))))

(defn add-link
  ([conn bucket name target]
   (add-link conn bucket name bucket target))
  ([conn link-bucket link-name ^String target-bucket ^String target-name]
   (-> (ObjectStore/.addLink
        (Connection/.objectStore (:conn @conn) link-bucket)
        link-name
        (ObjectInfo$Builder/.build (ObjectInfo/builder target-bucket target-name)))
       ObjectInfo->map)))

(defn add-bucket-link [conn link-bucket link-name target-bucket]
  (ObjectStore/.addBucketLink
   (Connection/.objectStore (:conn @conn) link-bucket)
   link-name
   (Connection/.objectStore (:conn @conn) target-bucket)))
