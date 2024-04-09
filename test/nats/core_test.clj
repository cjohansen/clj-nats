(ns nats.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [nats.core :as sut])
  (:import (io.nats.client.impl NatsMessage)))

(deftest build-message-test
  (testing "Constructs message object"
    (is (instance? NatsMessage (sut/build-message {:subject "test.subject"}))))

  (testing "Sets content-type header"
    (is (= (-> (sut/build-message
                {:subject "test.subject"
                 :data {:edn "Data"}})
               .getHeaders
               sut/headers->map)
           {"content-type" ["application/edn"]}))))
