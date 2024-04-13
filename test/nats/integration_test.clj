(ns nats.integration-test
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
            [java-time-literals.core]
            [nats.consumer :as consumer]
            [nats.core :as nats]
            [nats.message :as message]
            [nats.stream :as stream])
  (:import (java.time Instant)))

:java-time-literals.core/keep
(set! *print-namespace-maps* false)

(defn run-pubsub-scenario []
  (let [conn (nats/connect "nats://localhost:4222")
        subscription (nats/subscribe conn "clj-nats.chat.>")
        messages (atom [])
        running? (atom true)]
    (a/thread
      (loop []
        (when-let [msg (nats/pull-message subscription 500)]
          (swap! messages conj msg))
        (if @running?
          (recur)
          (nats/unsubscribe subscription))))

    (nats/publish conn
      {::message/subject "clj-nats.chat.general.christian"
       ::message/data {:message "Hello world!"}})

    (nats/publish conn
      {::message/subject "clj-nats.chat.general.rubber-duck"
       ::message/data {:message "Hi there, fella"}})

    (Thread/sleep 250)
    (reset! running? false)
    (nats/close conn)
    {:messages @messages}))

(deftest pubsub-test
  (testing "Receives messages"
    (is (= (->> (run-pubsub-scenario)
                :messages
                (map #(dissoc % ::message/SID)))
           [{::message/jet-stream? false
             ::message/status-message? false
             ::message/headers {"content-type" ["application/edn"]}
             ::message/consume-byte-count 98
             ::message/reply-to nil
             ::message/subject "clj-nats.chat.general.christian"
             ::message/has-headers? true
             ::message/data {:message "Hello world!"}}
            {::message/jet-stream? false
             ::message/status-message? false
             ::message/headers {"content-type" ["application/edn"]}
             ::message/consume-byte-count 103
             ::message/reply-to nil
             ::message/subject "clj-nats.chat.general.rubber-duck"
             ::message/has-headers? true
             ::message/data {:message "Hi there, fella"}}]))))

(def stream-data (atom nil))

(defn run-stream-scenario [& [{:keys [force?]}]]
  (when (or force? (nil? @stream-data))
    (let [conn (nats/connect "nats://localhost:4222")
          sess-id (random-uuid)
          stream-name (str "clj-nats-" sess-id)]
      (try
        (stream/create-stream conn
          {:nats.stream/name stream-name
           :nats.stream/description "A test stream"
           :nats.stream/subjects #{"clj-nats.stream.>"}
           :nats.stream/retention-policy :nats.retention-policy/limits
           :nats.stream/allow-direct-access? true
           :nats.stream/allow-rollup? false
           :nats.stream/deny-delete? false
           :nats.stream/deny-purge? false
           :nats.stream/max-age 1000
           :nats.stream/max-bytes 10000
           :nats.stream/max-consumers 10
           :nats.stream/max-messages 20
           :nats.stream/max-messages-per-subject 5
           :nats.stream/max-msg-size 200
           :nats.stream/replicas 1})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.test.1"
           :nats.message/data {:message "Number 1"}}
          {:stream stream-name})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.test.2"
           :nats.message/data {:message "Number 2"}}
          {:stream stream-name})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.test.3"
           :nats.message/data {:message "Number 3"}}
          {:stream stream-name})

        (Thread/sleep 50)

        (swap! stream-data assoc
               :cluster-info (stream/get-cluster-info conn stream-name)
               :stream-config (stream/get-stream-config conn stream-name)
               :mirror-info (stream/get-mirror-info conn stream-name)
               :stream-state (stream/get-stream-state conn stream-name)
               :stream-info (stream/get-stream-info conn stream-name)
               :stream-names (stream/get-stream-names conn)
               :streams (stream/get-streams conn)
               :account-statistics (stream/get-account-statistics conn)
               :first-message (stream/get-first-message conn stream-name "clj-nats.stream.test.*")
               :last-message (stream/get-last-message conn stream-name "clj-nats.stream.test.*")
               :message-2 (stream/get-message conn stream-name 2)
               :next-message (stream/get-next-message conn stream-name 2 "clj-nats.stream.test.*"))

        (stream/delete-message conn stream-name 1)
        (Thread/sleep 50)
        (swap! stream-data assoc
               :first-message-post-delete (stream/get-first-message conn stream-name "clj-nats.stream.test.*"))

        (stream/purge-stream conn stream-name)
        (Thread/sleep 50)
        (swap! stream-data assoc
               :first-message-post-purge-error
               (try
                 (stream/get-first-message conn stream-name "clj-nats.stream.test.*")
                 (catch Exception e
                   e)))
        (finally
          (stream/delete-stream conn stream-name)
          (Thread/sleep 50)
          (swap! stream-data assoc :streams-post-delete-stream (stream/get-stream-names conn))))

      (nats/close conn)
      nil))
  @stream-data)

(deftest stream-management-test
  (testing "Inspects stream cluster info"
    (is (-> (:cluster-info (run-stream-scenario))
            :nats.cluster/leader
            string?)))

  (testing "Inspects stream configuration"
    (is (= (dissoc (:stream-config (run-stream-scenario))
                   :nats.stream/name)
           {:nats.stream/allow-direct? true
            :nats.stream/max-msgs 20
            :nats.stream/no-ack? false
            :nats.stream/max-msgs-per-subject 5
            :nats.stream/deny-purge? false
            :nats.stream/first-sequence 1
            :nats.stream/subjects #{"clj-nats.stream.>"}
            :nats.stream/max-msg-size 200
            :nats.stream/discard-new-per-subject? false
            :nats.stream/mirror-direct? false
            :nats.stream/compression-option :nats.compression-option/none
            :nats.stream/discard-policy :nats.discard-policy/old
            :nats.stream/allow-rollup? false
            :nats.stream/max-age #time/dur "PT1S"
            :nats.stream/sealed? false
            :nats.stream/replicas 1
            :nats.stream/duplicate-window #time/dur "PT1S"
            :nats.stream/retention-policy :nats.retention-policy/limits
            :nats.stream/metadata {}
            :nats.stream/max-consumers 10
            :nats.stream/deny-delete? false
            :nats.stream/description "A test stream"
            :nats.stream/max-bytes 10000
            :nats.stream/consumer-limits {:nats.limits/max-ack-pending -1}
            :nats.stream/storage-type :nats.storage-type/file})))

  (testing "Inspects stream mirror info"
    (is (nil? (:mirror-info (run-stream-scenario)))))

  (testing "Returns stream state last/first time as instants"
    (is (let [res (:stream-state (run-stream-scenario))]
          (is (instance? Instant (:nats.stream/first-time res)))
          (is (instance? Instant (:nats.stream/last-time res))))))

  (testing "Inspects stream state"
    (is (= (dissoc (:stream-state (run-stream-scenario))
                   :nats.stream/first-time
                   :nats.stream/last-time)
           {:nats.stream/deleted-count 0
            :nats.stream/subjects #{}
            :nats.stream/byte-count 357
            :nats.stream/subject-count 3
            :nats.stream/consumer-count 0
            :nats.stream/first-sequence-number 1
            :nats.stream/deleted #{}
            :nats.stream/message-count 3})))

  (testing "Returns stream info create time and timestamp as instants"
    (is (let [res (:stream-info (run-stream-scenario))]
          (is (instance? Instant (:nats.stream/create-time res)))
          (is (instance? Instant (:nats.stream/timestamp res))))))

  (testing "Gets all the stuff in stream info"
    (is (= (:nats.stream/configuration (:stream-info (run-stream-scenario)))
           (:stream-config (run-stream-scenario)))))

  (testing "Stream message received-at is instant"
    (is (instance? Instant
                   (->> (run-stream-scenario)
                        :first-message
                        :nats.message/received-at))))

  (testing "Gets the first stream message"
    (is (= (dissoc (:first-message (run-stream-scenario))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 1"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 1
            :nats.message/subject "clj-nats.stream.test.1"})))

  (testing "Gets the last stream message"
    (is (= (dissoc (:last-message (run-stream-scenario))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 3"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 3
            :nats.message/subject "clj-nats.stream.test.3"})))

  (testing "Gets specific stream message"
    (is (= (dissoc (:message-2 (run-stream-scenario))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 2"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 2
            :nats.message/subject "clj-nats.stream.test.2"})))

  (testing "Gets specific stream message after seq-n"
    (is (= (dissoc (:next-message (run-stream-scenario))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 2"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 2
            :nats.message/subject "clj-nats.stream.test.2"})))

  (testing "Gets all stream names from the server"
    (is (< 0 (->> (run-stream-scenario)
                  :stream-names
                  (filter #(re-find #"^clj-nats-.*" %))
                  count))))

  (testing "Gets all streams from the server"
    (let [res (run-stream-scenario)]
      (is (= (->> (:streams res)
                  (filter (comp #(re-find #"^clj-nats-.*" %)
                                :nats.stream/name
                                :nats.stream/configuration))
                  first
                  :nats.stream/configuration)
             (:stream-config res)))))

  (testing "Gets account statistics from the server"
    (is (not (nil? (->> (:account-statistics (run-stream-scenario))
                        :nats.account/api-stats)))))

  (testing "Gets the first message after deleting the original first message"
    (is (= (dissoc (:first-message-post-delete (run-stream-scenario))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 2"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 2
            :nats.message/subject "clj-nats.stream.test.2"})))

  (testing "first-message errors after stream purge"
    (is (instance?
         io.nats.client.JetStreamApiException
         (:first-message-post-purge-error (run-stream-scenario)))))

  (testing "Stream is removed after deleting streams (D'OH!)"
    (is (= 0 (->> (run-stream-scenario)
                  :streams-post-delete-stream
                  (filter #(re-find #"^clj-nats-.*" %))
                  count)))))

(def consumer-data (atom nil))

(defn remove-randomness [{:keys [stream-name consumer-name] :as data}]
  (let [stream-re (re-pattern stream-name)
        consumer-re (re-pattern consumer-name)]
    (walk/postwalk
     (fn [x]
       (println (type x) x)
       (cond
         (= x stream-name) "TEST_STREAM_NAME"
         (= x consumer-name) "TEST_CONSUMER_NAME"
         (string? x) (-> x
                         (str/replace stream-re "TEST_STREAM_NAME")
                         (str/replace consumer-re "TEST_CONSUMER_NAME"))
         :else x))
     (dissoc data :stream-name :consumer-name))))

(defn run-consumer-scenario [& [{:keys [force?]}]]
  (when (or force? (nil? @consumer-data))
    (let [conn (nats/connect "nats://localhost:4222")
          stream-name (str "clj-nats-" (random-uuid))
          consumer-name (str "clj-nats-" (random-uuid))]
      (reset! consumer-data {:stream-name stream-name
                             :consumer-name consumer-name
                             :messages []})
      (try
        (stream/create-stream conn
          {:nats.stream/name stream-name
           :nats.stream/description "A test stream"
           :nats.stream/subjects #{"clj-nats.stream.>"}
           :nats.stream/retention-policy :nats.retention-policy/limits
           :nats.stream/allow-direct-access? true
           :nats.stream/allow-rollup? false
           :nats.stream/deny-delete? false
           :nats.stream/deny-purge? false
           :nats.stream/max-age 1000
           :nats.stream/max-bytes 10000
           :nats.stream/max-consumers 10
           :nats.stream/max-messages 20
           :nats.stream/max-messages-per-subject 5
           :nats.stream/max-msg-size 200
           :nats.stream/replicas 1})

        (consumer/create-consumer conn
          {:nats.consumer/name consumer-name
           :nats.consumer/stream-name stream-name
           :nats.consumer/ack-policy :nats.ack-policy/explicit
           :nats.consumer/description "Primary stream consumer"
           :nats.consumer/durable? true
           :nats.consumer/deliver-policy :nats.deliver-policy/all
           :nats.consumer/filter-subjects #{"clj-nats.stream.a.*"}})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.a.1"
           :nats.message/data {:message "Message A.1"}}
          {:stream stream-name})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.b.1"
           :nats.message/data {:message "Message B.1"}}
          {:stream stream-name})

        (stream/publish conn
          {:nats.message/subject "clj-nats.stream.a.2"
           :nats.message/data {:message "Message A.2"}}
          {:stream stream-name})

        (swap! consumer-data assoc
               :pre-consumer-info (consumer/get-consumer-info conn stream-name consumer-name)
               :consumer-names (consumer/get-consumer-names conn stream-name)
               :consumers (consumer/get-consumers conn stream-name))

        (let [subscription (consumer/subscribe conn stream-name consumer-name)
              message (consumer/pull-message subscription 10)]
          (swap! consumer-data update :messages conj message)
          (consumer/nak conn message)

          (let [message (consumer/pull-message subscription 10)]
            (swap! consumer-data update :messages conj message)
            (consumer/ack conn message))

          ;; Should be the first message again because it was nak'd the first
          ;; time
          (let [message (consumer/pull-message subscription 10)]
            (swap! consumer-data update :messages conj message)
            (consumer/ack conn message))

          ;; No more relevant messages for this consumer
          (swap! consumer-data update :messages conj
                 (or (consumer/pull-message subscription 10)
                     :stream-empty))

          (consumer/unsubscribe subscription))

        (finally
          (consumer/delete-consumer conn stream-name consumer-name)
          (stream/delete-stream conn stream-name)))
      (nats/close conn)))
  (remove-randomness @consumer-data))

(deftest consumer-test
  (testing "Consumer info uses instants for creation-time and timestamp"
    (is (->> (-> (run-consumer-scenario)
                 :pre-consumer-info
                 (select-keys [:nats.consumer/timestamp
                               :nats.consumer/creation-time])
                 vals)
             (every? #(instance? Instant %)))))

  (testing "Gets consumer info before consuming messages"
    (is (= (-> (run-consumer-scenario)
               :pre-consumer-info
               (dissoc :nats.consumer/timestamp
                       :nats.consumer/creation-time))
           {:nats.consumer/stream-name "TEST_STREAM_NAME"
            :nats.consumer/name "TEST_CONSUMER_NAME"
            :nats.consumer/pause-remaining nil
            :nats.consumer/push-bound? false
            :nats.consumer/num-waiting 0
            :nats.consumer/ack-floor nil
            :nats.consumer/num-pending 2
            :nats.consumer/paused false
            :nats.consumer/calculated-pending 2
            :nats.consumer/redelivered 0
            :nats.consumer/ack-pending 0
            :nats.consumer/delivered nil
            :nats.consumer/cluster-info nil
            :nats.consumer/consumer-configuration
            {:nats.consumer/consumer-name "TEST_CONSUMER_NAME"
             :nats.consumer/durable "TEST_CONSUMER_NAME"
             :nats.consumer/start-time nil
             :nats.consumer/deliver-group nil
             :nats.consumer/description "Primary stream consumer"
             :nats.consumer/backoff nil
             :nats.consumer/ack-policy :nats.ack-policy/explicit
             :nats.consumer/filter-subjects ["clj-nats.stream.a.*"]
             :nats.consumer/deliver-subject nil
             :nats.consumer/max-bytes -1
             :nats.consumer/headers-only? false
             :nats.consumer/flow-control-was-set? false
             :nats.consumer/max-ack-pending-was-set? true
             :nats.consumer/metadata-was-set? false
             :nats.consumer/mem-storage? false
             :nats.consumer/num-replicas 0
             :nats.consumer/flow-control? false
             :nats.consumer/num-replicas-was-set? true
             :nats.consumer/idle-heartbeat nil
             :nats.consumer/replay-policy-was-set? true
             :nats.consumer/metadata {}
             :nats.consumer/sample-frequency nil
             :nats.consumer/rate-limit-was-set? false
             :nats.consumer/inactve-threshold nil
             :nats.consumer/max-deliver-was-set? true
             :nats.consumer/pause-until nil
             :nats.consumer/max-expires nil
             :nats.consumer/max-bytes-was-set? false
             :nats.consumer/max-batch -1
             :nats.consumer/deliver-policy :nats.deliver-policy/all
             :nats.consumer/has-multiple-filter-subjects? false
             :nats.consumer/max-pull-waiting-was-set? true
             :nats.consumer/headers-only-was-set? false
             :nats.consumer/replay-policy :nats.replay-policy/limits
             :nats.consumer/max-batch-was-set? false
             :nats.consumer/deliver-policy-was-set? true
             :nats.consumer/start-sequence 0
             :nats.consumer/ack-wait #time/dur "PT30S"
             :nats.consumer/rate-limit 0
             :nats.consumer/max-deliver -1
             :nats.consumer/ack-policy-was-set? true
             :nats.consumer/mem-storage-was-set? false
             :nats.consumer/filter-subject "clj-nats.stream.a.*"
             :nats.consumer/max-pull-waiting 512
             :nats.consumer/backoff-was-set? false
             :nats.consumer/start-seq-was-set? false
             :nats.consumer/max-ack-pending 1000}})))

  (testing "Retrieves consumer names"
    (is (->> (run-consumer-scenario)
             :consumer-names
             (filter #{"TEST_CONSUMER_NAME"})
             seq)))

  (testing "Retrieves consumers"
    (is (->> (run-consumer-scenario)
             :consumers
             (filter (comp #{"TEST_CONSUMER_NAME"} :nats.consumer/name))
             seq)))

  (testing "Uses instants for message timestamps"
    (is (->> (run-consumer-scenario)
             :messages
             first
             :nats.message/metadata
             :nats.stream.meta/timestamp
             (instance? Instant))))

  (testing "Stream messages have reply-to"
    (is (->> (run-consumer-scenario)
             :messages
             first
             :nats.message/reply-to
             (re-find #"\$JS\.ACK\.TEST_STREAM_NAME\.TEST_CONSUMER_NAME\."))))

  (testing "Converts message to map"
    (is (= (-> (run-consumer-scenario)
               :messages
               first
               (dissoc :nats.message/reply-to)
               (update :nats.message/metadata dissoc :nats.stream.meta/timestamp))
           {:nats.message/SID "2"
            :nats.message/has-headers? true
            :nats.message/subject "clj-nats.stream.a.1"
            :nats.message/jet-stream? true
            :nats.message/consume-byte-count 212
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/data {:message "Message A.1"}
            :nats.message/status-message? false
            :nats.message/metadata
            {:nats.stream.meta/stream "TEST_STREAM_NAME"
             :nats.stream.meta/consumer "TEST_CONSUMER_NAME"
             :nats.stream.meta/delivered-count 1
             :nats.stream.meta/stream-sequence 1
             :nats.stream.meta/consumer-sequence 1
             :nats.stream.meta/pending-count 1}})))

  (testing "Receives messages as expected, including redelivery of naked message"
    (is (= (->> (run-consumer-scenario)
                :messages
                (map #(if (map? %)
                        ((juxt :nats.message/SID
                               :nats.message/subject
                               (comp :nats.stream.meta/stream-sequence :nats.message/metadata)
                               :nats.message/data)
                         %)
                        %)))
           [["2" "clj-nats.stream.a.1" 1 {:message "Message A.1"}]
            ["2" "clj-nats.stream.a.2" 3 {:message "Message A.2"}]
            ["2" "clj-nats.stream.a.1" 1 {:message "Message A.1"}]
            :stream-empty]))))
