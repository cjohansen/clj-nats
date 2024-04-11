(ns nats.integration-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [nats.core :as nats]))

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
      {:subject "chat.general.christian"
       :data {:message "Hello world!"}})

    (nats/publish conn
      {:subject "chat.general.rubber-duck"
       :data {:message "Hi there, fella"}})

    (Thread/sleep 250)
    (reset! running? false)
    (nats/close conn)
    {:messages @messages}))

(deftest pubsub-test
  (testing "Receives messages"
    (is (= (->> (run-pubsub-example)
                :messages
                (map #(dissoc % :SID)))
           [{:jet-stream? false
             :status-message? false
             :headers {"content-type" ["application/edn"]}
             :consume-byte-count 89
             :reply-to nil
             :subject "chat.general.christian"
             :has-headers? true
             :data {:message "Hello world!"}}
            {:jet-stream? false
             :status-message? false
             :headers {"content-type" ["application/edn"]}
             :consume-byte-count 94
             :reply-to nil
             :subject "chat.general.rubber-duck"
             :has-headers? true
             :data {:message "Hi there, fella"}}]))))
