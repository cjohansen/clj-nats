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
  (testing "joins parts into subject"
    (is (= (sut/create-subject ["clj-nats" "christian" "command"])
           "clj-nats.christian.command")))

  (testing "replaces not-recommended characters with a default"
    (is (= (sut/create-subject ["clj-nats.*.command.>"])
           "clj-nats_command_")))

  (testing "replaces not-recommended characters with a chosen replacement"
    (is (= (sut/create-subject ["clj-nats.*.command.>"] "R")
           "clj-natsRcommandR")))

  (testing "replaces not-recommended characters with a function of replaced characters"
    (is (= (sut/create-subject ["clj-nats.*.command.>"]
                               #(apply str (repeat (count %) "X")))
           "clj-natsXXXcommandXX"))))
