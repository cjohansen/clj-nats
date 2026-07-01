(ns nats.object-store-test
  (:require [clojure.test :refer [deftest is]]
            [nats.core :as nats]
            [nats.object-store :as object-store])
  (:import (java.util Arrays)))

(defonce connection (nats/connect "nats://localhost:4222"))
(def bucket-name "clj-nats-object-store-testdata")
(object-store/create-bucket connection {:nats.object-store/bucket-name bucket-name})
#_(object-store/delete-bucket connection bucket-name) ;; (should you want to start fresh)

(deftest put-get-bytes
  (let [message-bytes (String/.getBytes (str "Hello, " (rand-int 1000) "th world!") "UTF-8")]
    (object-store/put-bytes connection bucket-name "bytes-message.txt" message-bytes)
    (is (Arrays/equals message-bytes
                       (object-store/get-bytes connection bucket-name "bytes-message.txt")))))

(deftest put-get-str
  (let [message (str "Hello, " (rand-int 1000) "th world!")]
    (object-store/put-str connection bucket-name "str-message.txt" message)
    (is (= message (object-store/get-str connection bucket-name "str-message.txt")))))
