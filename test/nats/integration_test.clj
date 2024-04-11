(ns nats.integration-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [nats.core :as nats]
            [nats.message :as message]))

(defn run-pubsub-example []
  (let [conn (nats/connect "nats://localhost:4222")
        subscription (nats/subscribe conn "chat.>")
        messages (atom [])
        running? (atom true)]
    (a/thread
      (loop []
        (when-let [msg (nats/pull-message subscription 500)]
          (swap! messages conj msg))
        (if @running?
          (recur)
          (nats/unsubscribe subscription))))

    (nats/publish conn
      {::nats/subject "chat.general.christian"
       ::message/data {:message "Hello world!"}})

    (nats/publish conn
      {::nats/subject "chat.general.rubber-duck"
       ::message/data {:message "Hi there, fella"}})

    (Thread/sleep 250)
    (reset! running? false)
    (nats/close conn)
    {:messages @messages}))

(deftest pubsub-test
  (testing "Receives messages"
    (is (= (->> (run-pubsub-example)
                :messages
                (map #(dissoc % ::message/SID)))
           [{::message/jet-stream? false
             ::message/status-message? false
             ::message/headers {"content-type" ["application/edn"]}
             ::message/consume-byte-count 89
             ::message/reply-to nil
             ::message/subject "chat.general.christian"
             ::message/has-headers? true
             ::message/data {:message "Hello world!"}}
            {::message/jet-stream? false
             ::message/status-message? false
             ::message/headers {"content-type" ["application/edn"]}
             ::message/consume-byte-count 94
             ::message/reply-to nil
             ::message/subject "chat.general.rubber-duck"
             ::message/has-headers? true
             ::message/data {:message "Hi there, fella"}}]))))
