(ns nats.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [nats.core :as sut]))

(deftest covers-subject?-test
  (is (true? (sut/covers-subject? #{"clj-nats.subject"} "clj-nats.subject")))
  (is (false? (sut/covers-subject? #{"clj-nats.subject"} "clj-nats.other")))
  (is (true? (sut/covers-subject? #{"clj-nats.*"} "clj-nats.subject")))
  (is (false? (sut/covers-subject? #{"clj-nats.*"} "clj-nats.subject.more")))
  (is (false? (sut/covers-subject? #{"clj-nats.more.*"} "clj-nats.subject")))
  (is (true? (sut/covers-subject? #{"clj-nats.*.command"} "clj-nats.christian.command")))
  (is (false? (sut/covers-subject? #{"clj-nats.*.command.>"} "clj-nats.christian.command")))
  (is (true? (sut/covers-subject? #{"clj-nats.*.command.>"} "clj-nats.christian.command.ok")))
  (is (true? (sut/covers-subject? #{"clj-nats.>"} "clj-nats.subject")))
  (is (true? (sut/covers-subject? #{"clj-nats.>"} "clj-nats.subject.lol.haha")))
  (is (false? (sut/covers-subject? #{"clj-nats.>"} "clj-nats"))))

(deftest create-subject-test
  (is (= (sut/create-subject ["clj-nats" "christian" "command"] "-")
         "clj-nats.christian.command"))
  (testing "replaces disallowed characters with string"
    (is (= (sut/create-subject ["clj-nats.*.command.>"] "-")
           "clj-nats-command-")))
  (testing "replaces disallowed characters with a function of replaced characters"
    (is (= (sut/create-subject ["clj-nats.*.command.>"]
                               (fn [replaced]
                                 (apply str (repeat (count replaced) "X"))))
           "clj-natsXXXcommandXX"))))
