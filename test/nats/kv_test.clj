(ns nats.kv-test
  (:require [clojure.test :refer [deftest is testing]]
            [java-time-literals.core]
            [nats.kv :as sut])
  (:import (io.nats.client.api External KeyValueConfiguration Placement Republish
                               Source SubjectTransform)
           (java.time Instant)))

:java-time-literals.core/keep

(deftest build-options-test
  (testing "Builds External options"
    (is (instance?
         External
         (sut/build-external-options
          {:nats.external/api "lol"
           :nats.external/deliver "yes"}))))

  (testing "Builds SubjectTransform options"
    (is (instance?
         SubjectTransform
         (sut/build-subject-transform-options
          {:nats.subject-transform/source "A"
           :nats.subject-transform/destination "B"}))))

  (testing "Builds Source options"
    (is (instance?
         Source
         (sut/build-source-options
          {:nats.source/domain "domain"
           :nats.source/external {:nats.external/api "lol"
                                  :nats.external/deliver "yes"}
           :nats.source/filter-subject "chat.*"
           :nats.source/name "name"
           :nats.source/source-name "A"
           :nats.source/start-seq 0
           :nats.source/start-time (Instant/now)
           :nats.source/subject-transforms
           [{:nats.subject-transform/source "A"
             :nats.subject-transform/destination "B"}]}))))

  (testing "Builds Placement options"
    (is (instance?
         Placement
         (sut/build-placement-options
          {:nats.placement/cluster "cluster"
           :nats.placement/tags [:a :b]}))))

  (testing "Builds Republish options"
    (is (instance?
         Republish
         (sut/build-republish-options
          {:nats.republish/source "A"
           :nats.republish/destination "B"
           :nats.republish/headers-only? false}))))

  (testing "Builds KeyValue options"
    (is (instance?
         KeyValueConfiguration
         (sut/build-key-value-options
          {:nats.kv/sources [{:nats.source/name "A"}]
           :nats.kv/compression? true
           :nats.kv/description "A key/value store"
           :nats.kv/max-bucket-size 10
           :nats.kv/max-history-per-key 10
           :nats.kv/max-value-size 10
           :nats.kv/metadata {"Status" "Ok", :lol "Yes"}
           :nats.kv/mirror {:nats.source/name "B"}
           :nats.kv/bucket-name "Bucket"
           :nats.kv/placement {:nats.placement/cluster "cluster"
                               :nats.placement/tags [:a :b]}
           :nats.kv/replicas 2
           :nats.kv/republish {:nats.republish/source "A"
                               :nats.republish/destination "B"
                               :nats.republish/headers-only? false}
           :nats.kv/storage-type :nats.storage-type/file
           :nats.kv/ttl #time/dur "PT2S"})))))
