(ns nats.kv
  (:require [nats.core :as nats]
            [nats.stream :as stream])
  (:import (io.nats.client JetStreamOptions JetStreamOptions$Builder
                           KeyValueOptions KeyValueOptions$Builder)
           (io.nats.client.api External External$Builder
                               KeyValueConfiguration KeyValueConfiguration$Builder
                               Mirror Mirror$Builder
                               Placement Placement$Builder
                               Republish Republish$Builder
                               Source Source$Builder
                               SubjectTransform SubjectTransform$Builder)
           (java.util ArrayList)))

;; Build options

(defn build-jet-stream-options
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

(defn build-kvm-options [{::keys [stream-options domain prefix request-timeout]}]
  (cond-> ^KeyValueOptions$Builder (KeyValueOptions/builder)
    stream-options (.jetStreamOptions (build-jet-stream-options stream-options))
    domain (.jsDomain domain)
    prefix (.jsPrefix prefix)
    request-timeout (.jsRequestTimeout request-timeout)
    :then (.build)))

(defn build-external-options [{:nats.external/keys [api deliver]}]
  (cond-> ^External$Builder (External/builder)
    api (.api api)
    deliver (.deliver deliver)
    :then (.build)))

(defn build-subject-transform-options
  [{:nats.subject-transform/keys [destination source]}]
  (cond-> ^SubjectTransform$Builder (SubjectTransform/builder)
    destination (.destination destination)
    source (.source source)
    :then (.build)))

(defn build-source-options
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

(defn build-mirror-options
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

(defn build-placement-options
  [{:nats.placement/keys [cluster tags]}]
  (cond-> ^Placement$Builder (Placement/builder)
    cluster (.cluster cluster)
    (seq tags) (.tags (ArrayList. (map name tags)))
    :then (.build)))

(defn build-republish-options
  [{:nats.republish/keys [destination headers-only? source]}]
  (cond-> ^Republish$Builder (Republish/builder)
    destination (.destination destination)
    (boolean? headers-only?) (.headersOnly headers-only?)
    source (.source source)
    :then (.build)))

(defn build-key-value-options
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
    metadata (.metadata metadata)
    mirror (.mirror (build-mirror-options mirror))
    name (.name name)
    placement (.placement (build-placement-options placement))
    replicas (.replicas replicas)
    republish (.republish (build-republish-options republish))
    storage-type (.storageType (stream/storage-types storage-type))
    ttl (.ttl ttl)
    :then (.build)))

;; Public API
