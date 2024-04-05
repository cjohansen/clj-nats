(ns nats.dev
  (:require [nats.core :as nats]
            [nats.jet-stream :as jet-stream]))

(comment

  (def conn (nats/connect "nats://localhost:4222"))
  (jet-stream/get-config conn "m2n-export")

  )
