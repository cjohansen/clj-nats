(ns nats.core-publish-subscribe
  (:require [nats.core :as nats])
  (:import (java.io IOException)))

(try
  (def conn (nats/connect "nats://127.0.0.1:4222"))

  (nats/publish conn
    {:nats.message/subject "greet.joe"
     :nats.message/data "hello"})

  (def subscription (nats/subscribe conn "greet.*"))

  (def messages
    (future
      (loop [res []]
        (if-let [message (nats/pull-message subscription 10)]
          (recur (conj res message))
          res))))

  (nats/publish conn
    {:nats.message/subject "greet.bob"
     :nats.message/data "Hello"})

  (nats/publish conn
    {:nats.message/subject "greet.sue"
     :nats.message/data "Hi!"})

  (nats/publish conn
    {:nats.message/subject "greet.pam"
     :nats.message/data "Howdy!"})

  (Thread/sleep 50)

  (doseq [message @messages]
    (println (:nats.message/data message) "on" (:nats.message/subject message)))

  (catch InterruptedException e
    (.printStackTrace e))
  (catch IOException e
    (.printStackTrace e)))
