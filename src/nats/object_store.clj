(ns nats.object-store
  (:import (io.nats.client ObjectStoreOptions ObjectStoreOptions$Builder)
           (io.nats.client.api ObjectStoreConfiguration ObjectStoreConfiguration$Builder
                               ObjectStoreStatus))
  (:require [nats.stream :as stream]))

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

(defn ^:no-doc build-object-store-options [{::keys [stream-options domain prefix request-timeout]}]
  (cond-> ^ObjectStoreOptions$Builder (ObjectStoreOptions/builder)
    stream-options (.jetStreamOptions (stream/build-jet-stream-options stream-options))
    domain (.jsDomain domain)
    prefix (.jsPrefix prefix)
    request-timeout (.jsRequestTimeout request-timeout)
    :then (.build)))

(defn ^:no-doc build-object-store-configuration
  [{::keys [bucket-name
            compression?
            description
            max-bucket-size
            metadata
            placement
            replicas
            storage-type
            ttl]}]
  (cond-> ^ObjectStoreConfiguration$Builder (ObjectStoreConfiguration/builder)
    bucket-name (.name bucket-name)
    compression? (.compression compression?)
    description (.description description)
    max-bucket-size (.maxBucketSize max-bucket-size)
    metadata (.metadata (update-keys metadata clojure.core/name))
    placement (.placement placement)
    replicas (.replicas replicas)
    storage-type (.storageType storage-type)
    ttl (.ttl ttl)))

;; Helper functions

(defn ^:no-doc bucket-management [nats-conn]
  (let [{:keys [osbm conn object-store-options]} @nats-conn]
    (when-not osbm
      (->> (build-object-store-options object-store-options)
           (.objectStoreManagement conn)
           (swap! nats-conn assoc :osbm))))
  (:osbm @nats-conn))

(defn ^:no-doc object-store-management [nats-conn bucket-name]
  (let [{:keys [osm conn object-store-options]} @nats-conn]
    (when-not (get-in osm [bucket-name])
      (->> (build-object-store-options object-store-options)
           (.objectStore conn bucket-name)
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
      (.create (build-object-store-configuration config))
      object-store-status->map))

(defn ^:export get-bucket-status
  [conn bucket-name]
  (-> (bucket-management conn)
      (.getStatus bucket-name)
      object-store-status->map))

(defn ^:export delete-bucket
  "Delete a key/value bucket"
  [conn bucket-name]
  (.delete (bucket-management conn) bucket-name))

(defn ^:export get-bucket-statuses [conn]
  (->> (.getStatuses (bucket-management conn))
       (map object-store-status->map)
       set))
