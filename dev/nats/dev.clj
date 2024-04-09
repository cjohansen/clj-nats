(ns nats.dev
  (:require [nats.core :as nats]
            [nats.stream :as stream]))

(comment

  (def conn (nats/connect "nats://localhost:4222"))

  (stream/create-stream conn
    {:name "test-stream"
     :subjects ["test.work.>"]
     :allow-direct-access? true
     :retention-policy :nats.retention-policy/work-queue})

  (stream/get-config conn "test-stream")
  (stream/get-stream-info conn "test-stream")

  (nats/publish conn
    {:subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
     :data {:email/to "christian@cjohansen.no"
            :email/subject "Hello, world!"}})

  (stream/get-stream-names conn)
  (stream/get-streams conn)
  (stream/get-account-statistics conn)

  )
