(ns nats.core-publish-subscribe
  (:require [nats.core :as nats]
            [nats.message :as message])
  (:import (java.io IOException)))

(try
  (def conn (nats/connect "nats://127.0.0.1:4222"))

  (nats/publish conn
    {::nats/subject "greet.joe"
     ::message/data "hello"})

  (def subscription (nats/subscribe conn "greet.*"))

  (def messages
    (future
      (loop [res []]
        (if-let [message (nats/pull-message subscription 100)]
          (recur (conj res message))
          res))))

  (nats/publish conn
    {::nats/subject "greet.bob"
     :nats.message/data "Hello"})

  (nats/publish conn
    {::nats/subject "greet.sue"
     ::message/data "Hi!"})

  (nats/publish conn
    {::nats/subject "greet.pam"
     ::message/data "Howdy!"})

  (Thread/sleep 200)

  (doseq [message @messages]
    (println (::message/data message) "on" (::message/subject message)))

  (catch InterruptedException e
    (.printStackTrace e))
  (catch IOException e
    (.printStackTrace e)))
