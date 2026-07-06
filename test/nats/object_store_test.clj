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
             (select-keys [::object/bucket
                           ::object/chunks
                           ::object/digest
                           ::object/name
                           ::object/size-bytes]))
         {:nats.object/bucket store-name
          :nats.object/chunks 1
          :nats.object/digest "SHA-256=QcnWjqprxqQ-XfobO-nWBZBd9AEeZ-R4wVxgKFnE2BI="
          :nats.object/name "info.txt"
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

(defn has-bucket? [connection bucket-name]
  (boolean (some (comp #{bucket-name} :nats.object-store/bucket-name)
                 (object-store/get-bucket-statuses connection))))

(defn prepare-for-sealing [bucket-name]
  (when-not (has-bucket? connection bucket-name)
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

(def watch-store-name "clj-nats-object-store-testdata-watch")

(defn prepare-fresh-bucket [bucket-name]
  (when (has-bucket? connection bucket-name)
    (object-store/delete-bucket connection bucket-name))
  (object-store/create-bucket connection {:nats.object-store/bucket-name bucket-name}))

(deftest watch
  (testing "events prior to watch start"
    (let [infos (atom [])
          on-change #(swap! infos conj %)
          ready? (promise)
          on-end-of-data #(deliver ready? ::end-of-data)]
      ;; We prefer to write object store tests that don't care about other stuff
      ;; in the buckets, but this assumption doesn't hold for object store
      ;; watches, as object store watches can list events that happened back in
      ;; time, before the watch was created.
      (prepare-fresh-bucket watch-store-name)
      (object-store/put-str connection watch-store-name "hi1.txt" "hi there")
      (object-store/put-str connection watch-store-name "hi2.txt" "aloha")
      (with-open [_ (object-store/watch connection watch-store-name on-change {:on-end-of-data on-end-of-data})]
        (deref ready?)
        (is (= '({::object/digest "SHA-256=m5ah_h1UjLvJYMxqAoZmj9dKdjZnsGNm-yMkJp_KuqQ=",
                  ::object/name "hi1.txt"}
                 {::object/digest "SHA-256=AgapeEOxuk-7FH1HJVDsO17oqsrfNwdSIVckCUDRvr0=",
                  ::object/name "hi2.txt"})
               (map #(select-keys % [::object/digest ::object/name])
                    (deref infos)))))))

  (testing "events after watch start"
    (let [infos (atom [])
          on-change #(swap! infos conj %)
          _ (prepare-fresh-bucket watch-store-name)]
      (with-open [_ (object-store/watch connection watch-store-name on-change)]
        (object-store/put-str connection watch-store-name "hi1.txt" "hi there")
        (object-store/put-str connection watch-store-name "hi2.txt" "aloha")

        ;; "Why is object-store/list here?" Sorry :(. There's doesn't appear to
        ;; be any jnats operation to wait untill all watchers are up to speed.
        ;; Making a call to list objects appears to be sufficient.
        (object-store/list connection watch-store-name))
      (is (= '({::object/digest "SHA-256=m5ah_h1UjLvJYMxqAoZmj9dKdjZnsGNm-yMkJp_KuqQ=",
                ::object/name "hi1.txt"}
               {::object/digest "SHA-256=AgapeEOxuk-7FH1HJVDsO17oqsrfNwdSIVckCUDRvr0=",
                ::object/name "hi2.txt"})
             (map #(select-keys % [::object/digest ::object/name])
                  (deref infos))))))

  (testing "ignores events after watch stop"
    (let [infos (atom [])
          on-change #(swap! infos conj %)
          _ (prepare-fresh-bucket watch-store-name)]
      (with-open [_ (object-store/watch connection watch-store-name on-change)]
        (object-store/put-str connection watch-store-name "hi1.txt" "hi there")
        (object-store/put-str connection watch-store-name "hi2.txt" "aloha")
        (object-store/list connection watch-store-name))
      (object-store/put-str connection watch-store-name "ignored1.txt" "please ignore")
      (object-store/put-str connection watch-store-name "ignored2.txt" "yeah, ignore.")
      (is (= '({::object/digest "SHA-256=m5ah_h1UjLvJYMxqAoZmj9dKdjZnsGNm-yMkJp_KuqQ=",
                ::object/name "hi1.txt"}
               {::object/digest "SHA-256=AgapeEOxuk-7FH1HJVDsO17oqsrfNwdSIVckCUDRvr0=",
                ::object/name "hi2.txt"})
             (map #(select-keys % [::object/digest ::object/name])
                  (deref infos)))))))

(deftest link
  (object-store/put-str connection store-name "norge-brazil.txt" "final result: 2-1!")
  (object-store/add-link connection store-name "norgeskamp.txt.link" "norge-brazil.txt")

  (testing "resolve link"
    (is (= {:nats.object/bucket "clj-nats-object-store-testdata",
            :nats.object/name "norge-brazil.txt"}
           (object-store/resolve-link connection store-name "norgeskamp.txt.link"))))

  (testing "resolve nils if input isn't a link"
    (is (nil? (object-store/resolve-link connection store-name "norge-brazil.txt")))
    (is (nil? (object-store/resolve-link connection store-name "doesn't even exist"))))

  (testing "deleting a link"
    (object-store/add-link connection store-name "link-to-delete.txt.link" "norge-brazil.txt")
    (object-store/delete connection store-name "link-to-delete.txt.link")
    (is (nil? (object-store/resolve-link connection store-name "link-to-delete.txt.link")))
    (testing "... but the link target remains."
      (is (false? (::object/deleted? (object-store/get-info connection store-name "norge-brazil.txt")))))))

(def bucket-link-store-name "clj-nats-object-store-testdata-bucket-link")

(deftest bucket-link
  (prepare-fresh-bucket bucket-link-store-name)
  (object-store/add-bucket-link connection store-name "link-to-bucket.link" bucket-link-store-name)
  (is (= {:nats.object/bucket bucket-link-store-name}
         (object-store/resolve-link connection store-name "link-to-bucket.link"))))

(comment
  ;; Typical workflow: demonstrate an Object Store capability with the Java API,
  ;; then look for an alternative in idiomatic Clojure.

  (import '[io.nats.client Nats Connection ObjectStoreManagement ObjectStore Subscription]
          '[io.nats.client.api
            ObjectStoreConfiguration ObjectStoreStatus
            ObjectMeta ObjectMetaOptions
            ObjectLink
            ObjectInfo
            ObjectStoreWatcher ObjectStoreWatchOption
            Watcher]
          '[io.nats.client.impl
            AckType Headers NatsJetStreamMetaData NatsMessage NatsMessage$Builder
            NatsObjectStoreWatchSubscription]
          '[java.io ByteArrayInputStream ByteArrayOutputStream]
          '[java.lang String AutoCloseable]
          '[java.util Arrays])

  (def store (Connection/.objectStore (:conn @connection) store-name))

  )
