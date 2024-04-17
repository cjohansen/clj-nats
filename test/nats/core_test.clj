(ns nats.core-test
  (:require [clojure.test :refer [deftest is]]
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
