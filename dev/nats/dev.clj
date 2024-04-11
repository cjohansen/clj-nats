(ns nats.dev
  (:require [nats.core :as nats]
            [nats.stream :as stream]))

(defn generate-messages [conn subject-prefix n]
  (dotimes [i n]
    (stream/publish conn
      {:subject (str subject-prefix (random-uuid))
       :data {:message-idx i}})))

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

  (stream/create-stream conn
    {:name "other-stream"
     :subjects ["test.events.>"]
     :allow-direct-access? true
     :retention-policy :nats.retention-policy/limits})

  (stream/get-stream-config conn "test-stream")
  (stream/get-stream-info conn "test-stream")
  (stream/get-stream-info conn "other-stream")

  (stream/publish conn
    {:subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
     :data {:email/to "christian@cjohansen.no"
            :email/subject "Hello, world!"}})

  (stream/publish conn
    {:subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     :data {:user/email "christian@cjohansen.no"}})

  (stream/publish conn
    {:subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     :data {:user/email "christian.johansen@mattilsynet.no"}})

  (stream/get-stream-names conn)
  (stream/get-streams conn)
  (stream/get-account-statistics conn)
  (stream/get-first-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-first-message conn "test-stream" "test.work.email.*")
  (stream/get-last-message conn "test-stream" "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/get-message conn "test-stream" 3)
  (stream/get-next-message conn "test-stream" 2 "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed")
  (stream/create-consumer conn
    {:stream-name "test-stream"
     :consumer-name "worker"})
  (stream/get-consumer-info conn "test-stream" "worker")
  (stream/delete-consumer conn "test-stream" "worker")
  (stream/get-consumer-names conn "test-stream")
  (stream/get-consumers conn "test-stream")

  (stream/create-consumer conn
    {:stream-name "other-stream"
     :consumer-name "test-consumer"
     :durable? true
     :filter-subject "test.events.>"})

  (stream/get-consumer-info conn "other-stream" "test-consumer")

  (with-open [subscription (stream/subscribe conn "other-stream" "test-consumer")]
    (prn (stream/pull-message subscription 1000)))

  (def fetcher (stream/fetch conn "other-stream" "test-consumer" {:max-messages 10}))

  (def message (stream/fetch-next fetcher))
  (stream/ack conn message)




  (def subscription (stream/fetch conn "other-stream" "test-consumer" {:max-messages 10}))

  (def message (stream/fetch-next consumer))
  (stream/ack conn message)

  (when-let [msg (stream/fetch-next consumer)]
    [msg (stream/ack conn msg)])

  (def res (future (stream/fetch subscription 5 10000)))

  (def message @res)

  (generate-messages conn "test.events.lol." 10)

  (stream/publish conn
    {:subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     :data {:user/email "christian@cjohansen.no"}})

  (stream/publish conn
    {:subject "test.events.user-entered.12775c9e-f193-4b24-9297-e8f64536cdd8"
     :data {:user/email "christian.johansen@mattilsynet.no"}})


  )
