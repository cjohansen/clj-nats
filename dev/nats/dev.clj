(ns nats.dev
  (:require [nats.core :as nats]
            [nats.stream :as stream]))

(comment
  (set! *print-namespace-maps* false)
  (def conn (nats/connect "nats://localhost:4222"))

  (nats/publish conn
    {:subject "chat.general.christian"
     :data {:message "Hello world!"}})

  (stream/create-stream conn
    {:name "test-stream"
     :subjects ["test.work.>"]
     :allow-direct-access? true
     :retention-policy :nats.retention-policy/work-queue})

  (stream/get-config conn "test-stream")
  (stream/get-stream-info conn "test-stream")

  (stream/publish conn
    {:subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
     :data {:email/to "christian@cjohansen.no"
            :email/subject "Hello, world!"}})

  (stream/get-stream-names conn)
  (stream/get-streams conn)
  (stream/get-account-statistics conn)
  (stream/get-first-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-last-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-message conn "test-stream" 3)
  (stream/get-next-message conn "test-stream" 2 "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/add-consumer conn "test-stream" {:name "worker"})
  (stream/get-consumer-info conn "test-stream" "worker")
  (stream/delete-consumer conn "test-stream" "worker")
  (stream/get-consumer-names conn "test-stream")
  (stream/get-consumers conn "test-stream")

  )
