# Idiomatic Clojure wrapper for jnats

clj-nats is an idiomatic Clojure wrapper for the official
[NATS](https://nats.io) Java SDK [jnats](https://javadoc.io/doc/io.nats/jnats/).

```clj
no.cjohansen/clj-nats {:git/repo "https://github.com/cjohansen/clj-nats"
                       :sha "LATEST_SHA"}
```

The current jnats version is `2.17.4`.

## Rationale

clj-nats aims to be an easy to use Clojure client for NATS. It does this in a
few ways.

### Clojure data

All functions take native Clojure data as arguments instead of instances of
jnats option classes. Almost all functions return Clojure data instead of
instances of jnats data classes.

The only exceptions are Java time classes, as they are immutable values, and not
easily represented without their wrappers. Specifically, clj-nats uses
`java.time.Duration` (timeouts etc) and `java.time.Instant` (timestamps etc).
With [java-time-literals](https://github.com/magnars/java-time-literals) even
these will be represented as data literals.

clj-nats supports keywords for subjects and stream names.

### EDN messages

clj-nats automatically encodes and decodes EDN messages when appropriate. No
need to convert messages to binary by hand. clj-nats sets a header on your
messages to do this safely and transparently.

### Opinionated (smaller) feature subset

clj-nats wraps a subset of jnats. jnats offers many ways to solve most tasks.
clj-nats only provides the most flexible approach, leaving you with enough
leverage to cater to your own preferences. As an example, jnats has `publish`
and `publishAsync`, while clj-nats only provides `publish`. If you want async,
you can wrap it in a `future`, create a virtual thread, or choose any number of
other ways to publish off the main thread.

### Instants for timestamps

jnats uses `java.time.ZonedDateTime` for all timestamps with a GMT timezone.
Because timestamps are instants, this is an unnecessary detour, so clj-nats
operates strictly with `java.time.Instant`, both for inputs and outputs.

### Flat structure

clj-nats is organized in a flat structure loosely inspired by the `nats` CLI.
The goal is to make it easy to translate CLI examples to clj-nats usage.

## Usage

Create a connection:

```clj
(require '[nats.core :as nats])

(def conn (nats/connect "nats://localhost:4222"))
```

Publish a message (see below for publishing to streams):

```clj
(require '[nats.core :as nats]
         '[nats.message :as message])

(def conn (nats/connect "nats://localhost:4222"))

(nats/publish conn
  {::message/subject "chat.general.christian"
   ::message/data {:message "Hello world!"}
   ::message/headers {:user "Christian"}       ;; Optional
   ::message/reply-to "chat.general.replies"}) ;; Optional
```

Subscribing to messages (see below for consuming streams):

```clj
(require '[nats.core :as nats])

(def conn (nats/connect "nats://localhost:4222"))
(def subscription (nats/subscribe conn "chat.>"))

(def msg1 (nats/pull-message subscription 500)) ;; Wait up to 500ms
(def msg2 (nats/pull-message subscription 500)) ;; Wait up to 500ms
(nats/unsubscribe subscription)
```

Create a stream:

```clj
(require '[nats.core :as nats]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(stream/create-stream conn
  {::stream/name "test-stream"
   ::stream/subjects ["test.work.>"]
   ::stream/allow-direct-access? true
   ::stream/retention-policy :nats.retention-policy/work-queue})
```

Publish to a stream subject:

```clj
(require '[nats.core :as nats]
         '[nats.message :as message]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(stream/publish conn
  {::message/subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
   ::message/data {:email/to "christian@cjohansen.no"
                   :email/subject "Hello, world!"}})
```

Create a consumer:

```clj
(require '[nats.core :as nats]
         '[nats.consumer :as consumer]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(consumer/create-consumer conn
  {::consumer/stream-name :test-stream
   ::consumer/name :test-consumer
   ::consumer/durable? true
   ::consumer/filter-subject :test.work.>})

;; Review its configuration
(stream/get-consumer-info conn :test-stream :test-consumer)
```

Consume messages:

```clj
(require '[nats.core :as nats]
         '[nats.consumer :as consumer]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(with-open [subscription (consumer/subscribe conn :test-stream :test-consumer)]
  (let [message (consumer/pull-message subscription 1000)] ;; Wait for up to 1000ms
    (consumer/ack conn message)
    (prn message)))
```

Review stream configuration and state:

```clj
(require '[nats.stream :as stream])

(stream/get-stream-config conn :test-stream)
(stream/get-stream-info conn :test-stream)
(stream/get-stream-state conn :test-stream)
(stream/get-mirror-info conn :test-stream)
(stream/get-consumers conn :test-stream)
```

Peek at some messages from the stream:

```clj
(stream/get-first-message conn :test-stream :test.work.email.*)
(stream/get-last-message conn :test-stream :test.work.email.*)
(stream/get-message conn :test-stream 3)
(stream/get-next-message conn :test-stream 2 :test.work.email.*)
```

Get information from the server:

```clj
(require '[nats.stream :as stream])

(stream/get-streams conn)
(stream/get-account-statistics conn)
```
