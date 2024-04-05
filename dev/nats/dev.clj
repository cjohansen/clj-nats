(ns nats.dev
  (:require [nats.core :as nats]
            [nats.jet-stream :as jet-stream]))

(comment

  (def conn (nats/connect "nats://localhost:4222"))

  (jet-stream/create-stream conn
    {:name "test-stream"
     :subjects ["test.work.>"]
     :allow-direct-access? true
     :retention-policy :nats.retention-policy/work-queue})

  (jet-stream/get-config conn "test-stream")

  (nats/publish conn
    {:subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
     :data {:email/to "christian@cjohansen.no"
            :email/subject "Hello, world!"}})

  )
