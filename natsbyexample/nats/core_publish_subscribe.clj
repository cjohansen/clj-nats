(ns nats.core-publish-subscribe
  (:require [nats.core :as nats]
            [nats.message :as message])
  (:import (java.io IOException)))

(try
  (def conn (nats/connect "nats://127.0.0.1:4222"))

  (nats/publish conn
    {::message/subject "greet.joe"
     ::message/data "hello"})

  (def subscription (nats/subscribe conn "greet.*"))

  (def messages
    (future
      (loop [res []]
        (if-let [message (nats/pull-message subscription 10)]
          (recur (conj res message))
          res))))

  (nats/publish conn
    {::message/subject "greet.bob"
     ::message/data "Hello"})

  (nats/publish conn
    {::message/subject "greet.sue"
     ::message/data "Hi!"})

  (nats/publish conn
    {::message/subject "greet.pam"
     ::message/data "Howdy!"})

  (Thread/sleep 50)

  (doseq [message @messages]
    (println (::message/data message) "on" (::message/subject message)))

  (catch InterruptedException e
    (.printStackTrace e))
  (catch IOException e
    (.printStackTrace e)))
