(ns nats.dev
  (:require [nats.consumer :as consumer]
            [nats.core :as nats]
            [nats.message :as message]
            [nats.stream :as stream]))

(defn generate-messages [conn subject-prefix n]
  (dotimes [i n]
    (stream/publish conn
      {::message/subject (str subject-prefix (random-uuid))
       ::message/data {:message-idx i}})))

(comment
  (set! *print-namespace-maps* false)
  (def conn (nats/connect "nats://localhost:4222"))

  (nats/publish conn
    {::message/subject "chat.general.christian"
     ::message/data {:message "Hello world!!"}})

  (stream/create-stream conn
    {::stream/name "test-stream"
     ::stream/subjects #{"test.work.>"}
     ::stream/allow-direct-access? true
     ::stream/retention-policy :nats.retention-policy/work-queue})

  (stream/create-stream conn
    {::stream/name "other-stream"
     ::stream/subjects #{"test.events.>"}
     ::stream/allow-direct-access? true
     ::stream/retention-policy :nats.retention-policy/limits})

  (stream/get-stream-config conn "test-stream")
  (stream/get-stream-info conn "test-stream")
  (stream/get-stream-info conn "other-stream")

  (stream/publish conn
    {::message/subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
     ::message/data {:email/to "christian@cjohansen.no"
                     :email/subject "Hello, world!"}})

  (stream/publish conn
    {::message/subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     ::message/data {:user/email "christian@cjohansen.no"}})

  (stream/publish conn
    {::message/subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     ::message/data {:user/email "christian.johansen@mattilsynet.no"}})

  (stream/get-stream-names conn)
  (stream/get-streams conn)
  (stream/get-account-statistics conn)
  (stream/get-first-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-first-message conn "test-stream" "test.work.email.*")
  (stream/get-last-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-message conn "test-stream" 3)
  (stream/get-next-message conn "test-stream" 2 "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")

  (consumer/create-consumer conn
    {::consumer/stream-name "test-stream"
     ::consumer/name "worker"})

  (consumer/get-consumer-info conn "test-stream" "worker")
  (consumer/delete-consumer conn "test-stream" "worker")
  (consumer/get-consumer-names conn "test-stream")
  (consumer/get-consumers conn "test-stream")

  (consumer/create-consumer conn
    {::consumer/stream-name "other-stream"
     ::consumer/name "test-consumer"
     ::consumer/durable? true
     ::consumer/filter-subject "test.events.>"})

  (consumer/get-consumer-info conn "other-stream" "test-consumer")

  (with-open [subscription (consumer/subscribe conn "other-stream" "test-consumer")]
    (consumer/pull-message subscription 1000))

  )
