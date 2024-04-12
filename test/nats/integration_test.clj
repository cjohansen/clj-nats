(ns nats.integration-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [java-time-literals.core]
            [nats.core :as nats]
            [nats.message :as message]
            [nats.stream :as stream])
  (:import (java.time Instant)))

:java-time-literals.core/keep

(defn run-pubsub-example []
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
    (is (= (->> (run-pubsub-example)
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

(defn run-stream-example [& [{:keys [force?]}]]
  (when (or force? (nil? @stream-data))
    (let [conn (nats/connect "nats://localhost:4222")
          sess-id (random-uuid)
          stream-name (str "clj-nats-" sess-id)]
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

      (Thread/sleep 100)

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

      (stream/delete-stream conn stream-name)
      (nats/close conn)
      nil))
  @stream-data)

(deftest stream-management-test
  (testing "Inspects stream cluster info"
    (is (-> (:cluster-info (run-stream-example))
            :nats.cluster/leader
            string?)))

  (testing "Inspects stream configuration"
    (is (= (dissoc (:stream-config (run-stream-example))
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
    (is (nil? (:mirror-info (run-stream-example)))))

  (testing "Returns stream state last/first time as instants"
    (is (let [res (:stream-state (run-stream-example))]
          (is (instance? Instant (:nats.stream/first-time res)))
          (is (instance? Instant (:nats.stream/last-time res))))))

  (testing "Inspects stream state"
    (is (= (dissoc (:stream-state (run-stream-example))
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
    (is (let [res (:stream-info (run-stream-example))]
          (is (instance? Instant (:nats.stream/create-time res)))
          (is (instance? Instant (:nats.stream/timestamp res))))))

  (testing "Gets all the stuff in stream info"
    (is (= (:nats.stream/configuration (:stream-info (run-stream-example)))
           (:stream-config (run-stream-example)))))

  (testing "Stream message received-at is instant"
    (is (instance? Instant
                   (->> (run-stream-example)
                        :first-message
                        :nats.message/received-at))))

  (testing "Gets the first stream message"
    (is (= (dissoc (:first-message (run-stream-example))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 1"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 1
            :nats.message/subject "clj-nats.stream.test.1"})))

  (testing "Gets the last stream message"
    (is (= (dissoc (:last-message (run-stream-example))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 3"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 3
            :nats.message/subject "clj-nats.stream.test.3"})))

  (testing "Gets specific stream message"
    (is (= (dissoc (:message-2 (run-stream-example))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 2"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 2
            :nats.message/subject "clj-nats.stream.test.2"})))

  (testing "Gets specific stream message after seq-n"
    (is (= (dissoc (:next-message (run-stream-example))
                   :nats.message/received-at
                   :nats.message/stream)
           {:nats.message/data {:message "Number 2"}
            :nats.message/headers {"content-type" ["application/edn"]}
            :nats.message/last-seq -1
            :nats.message/seq 2
            :nats.message/subject "clj-nats.stream.test.2"})))

  (testing "Gets all stream names from the server"
    (is (< 0 (->> (run-stream-example)
                  :stream-names
                  (filter #(re-find #"^clj-nats-.*" %))
                  count))))

  (testing "Gets all streams from the server"
    (let [res (run-stream-example)]
      (is (= (->> (:streams res)
                  (filter (comp #(re-find #"^clj-nats-.*" %)
                                :nats.stream/name
                                :nats.stream/configuration))
                  first
                  :nats.stream/configuration)
             (:stream-config res)))))

  (testing "Gets account statistics from the server"
    (is (not (nil? (->> (:account-statistics (run-stream-example))
                        :nats.account/api-stats))))))
