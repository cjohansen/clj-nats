(ns nats.object-store-test
  (:require [clojure.test :refer [deftest is testing]]
            [nats.core :as nats]
            [nats.object :as-alias object]
            [nats.object-store :as object-store])
  (:import (java.util Arrays)))

(defonce connection (nats/connect "nats://localhost:4222"))
(def store-name "clj-nats-object-store-testdata")
(object-store/create-bucket connection {:nats.object-store/bucket-name store-name})
#_(object-store/delete-bucket connection bucket-name) ;; (should you want to start fresh)

(deftest put-get-bytes
  (let [message-bytes (String/.getBytes (str "Hello, " (rand-int 1000) "th world!") "UTF-8")]
    (object-store/put-bytes connection store-name "bytes-message.txt" message-bytes)
    (is (Arrays/equals message-bytes
                       (object-store/get-bytes connection store-name "bytes-message.txt")))))

(deftest put-get-str
  (let [message (str "Hello, " (rand-int 1000) "th world!")]
    (object-store/put-str connection store-name "str-message.txt" message)
    (is (= message (object-store/get-str connection store-name "str-message.txt")))))

(deftest ObjectInfo->map
  (object-store/put-str connection store-name "ObjectInfo->map.txt" "a text file")
  (is (contains? (into #{}
                       (map :nats.object/name)
                       (object-store/list connection store-name))
                 "ObjectInfo->map.txt")))

(deftest get-info
  (object-store/put-str connection store-name "info.txt" "information about this and that")
  (is (= (-> (object-store/get-info connection store-name "info.txt")
             (select-keys [::object/digest
                           ::object/chunks
                           ::object/name
                           ::object/size-bytes]))
         {:nats.object/digest "SHA-256=QcnWjqprxqQ-XfobO-nWBZBd9AEeZ-R4wVxgKFnE2BI=",
          :nats.object/chunks 1,
          :nats.object/name "info.txt",
          :nats.object/size-bytes 31}))

  (testing "put-str returns mostly the same as get-info"
    (let [message (str "Message " (rand-int 1000))]
      ;; Why dissoc ::object/modified-zdt, you say?
      ;;
      ;; Very funny you should ask. You see, information from the put-str and
      ;; the get-info call is identical (including SHA256 digest), but the
      ;; modified timestamp is different. In my testing, the put-str call
      ;; returns a timestamp *after* the get-info call.
      (is (= (-> (object-store/put-str connection store-name "info2.txt" message)
                 (dissoc ::object/modified-zdt))
             (-> (object-store/get-info connection store-name "info2.txt")
                 (dissoc ::object/modified-zdt)))))))

(deftest delete
  (testing "deletion"
    (object-store/put-str connection store-name "deleted.txt" "to be deleted!")
    (object-store/delete connection store-name "deleted.txt")
    (is (true? (::object/deleted?
                (object-store/get-info connection
                                       store-name
                                       "deleted.txt"
                                       {::object/include-deleted? true})
                "not found"))))

  (testing "deletion is idempotent if we ignore the modified-zdt field (which is weird)"
    (object-store/put-str connection store-name "deleted2.txt" "double time!")
    (is (= (-> (object-store/delete connection store-name "deleted2.txt")
               (dissoc ::object/modified-zdt))
           (-> (object-store/delete connection store-name "deleted2.txt")
               (dissoc ::object/modified-zdt))))))

(defn prepare-for-sealing [bucket-name]
  (when-not (some (comp #{bucket-name} :nats.object-store/bucket-name)
                  (object-store/get-bucket-statuses connection))
    (object-store/create-bucket connection {:nats.object-store/bucket-name bucket-name})
    (object-store/put-str connection bucket-name "before-seal.txt" "Written before the sealing"))
  (object-store/get-bucket-status connection bucket-name))

(def sealed-store-name "clj-nats-object-store-sealed")

(deftest seal
  (prepare-for-sealing sealed-store-name)
  (object-store/seal! connection sealed-store-name)
  (is (:nats.object-store/sealed? (object-store/get-bucket-status connection sealed-store-name)))

  (testing "files written before sealing are available"
    (is (object-store/get-info connection sealed-store-name "before-seal.txt")))

  (testing "attempting mutation after sealing gives an exception"
    (is (= :exception
           (try (object-store/put-str
                 connection sealed-store-name
                 "after-seal.txt" "Written after the sealing")
                :success
                (catch Exception _ :exception))))))

(comment
  ;; Typical workflow: demonstrate an Object Store capability with the Java API,
  ;; then look for an alternative in idiomatic Clojure.

  (import '[io.nats.client Nats Connection ObjectStoreManagement ObjectStore]
          '[io.nats.client.api ObjectStoreConfiguration ObjectStoreStatus ObjectMeta ObjectInfo]
          '[io.nats.client.impl
            AckType Headers NatsJetStreamMetaData NatsMessage NatsMessage$Builder]
          '[java.io ByteArrayInputStream ByteArrayOutputStream]
          '[java.lang String]
          '[java.util Arrays])

  (def store (Connection/.objectStore (:conn @connection) store-name))

  )
