(ns nats.object-store
  (:import (io.nats.client Connection ObjectStore ObjectStoreManagement ObjectStoreOptions ObjectStoreOptions$Builder)
           (io.nats.client.api ObjectStoreConfiguration ObjectStoreConfiguration$Builder
                               ObjectStoreStatus Placement StorageType)
           (java.io ByteArrayOutputStream)
           (java.time Duration)
           (java.util Map))
  (:require [clojure.core.async :refer [buffer]]
            [nats.stream :as stream]))

#_(set! *warn-on-reflection* true)

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

(defn put-bytes [conn bucket ^String object-name ^bytes bytes]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)]
    (ObjectStore/.put object-store object-name bytes)))

(defn put-str [conn bucket ^String object-name ^String s]
  (put-bytes conn bucket object-name (String/.getBytes s "UTF-8")))

(defn get-bytes ^bytes [conn bucket ^String object-name]
  (let [object-store (Connection/.objectStore (:conn @conn) bucket)
        buffer (ByteArrayOutputStream/new)]
    (ObjectStore/.get object-store object-name buffer)
    (ByteArrayOutputStream/.toByteArray buffer)))

(defn get-str [conn bucket ^String object-name]
  (String. (get-bytes conn bucket object-name) "UTF-8"))
