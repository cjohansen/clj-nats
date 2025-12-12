# clj-nats: Clojure NATS client library

clj-nats is a Clojure client library for [NATS](https://nats.io). It's based on
the official Java SDK [jnats](https://javadoc.io/doc/io.nats/jnats/).

Install:

```clj
no.cjohansen/clj-nats {:git/url "https://github.com/cjohansen/clj-nats"
                       :git/sha "LATEST_SHA"}
```

The current jnats version is `2.24.1`.

## Status

The API is stable but incomplete. Specifically, the object store APIs are not
implemented.

- [x] PubSub
- [x] Streams
- [x] Consumers
- [x] Request/response
- [x] Key/value store (in progress)
- [ ] Object store

## Rationale

clj-nats aims to be an easy to use Clojure client for NATS. It does this in a
few ways.

### Clojure data

All functions take native Clojure data as arguments instead of instances of
jnats option classes. Almost all functions return Clojure data instead of
instances of jnats data classes.

The only exceptions are Java time classes, as they are already immutable values.
Specifically, clj-nats uses `java.time.Duration` (timeouts etc) and
`java.time.Instant` (timestamps etc). With
[java-time-literals](https://github.com/magnars/java-time-literals) even these
will be represented as data literals.

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

[Full API docs on cljdoc.org](https://cljdoc.org/d/no.cjohansen/clj-nats/).

### Create a connection

```clj
(require '[nats.core :as nats])

(def conn (nats/connect "nats://localhost:4222"))
```

See also: [authenticated connections](#auth).

### Pubsub

Publishing a message (see below for publishing to streams):

```clj
(require '[nats.core :as nats])

(def conn (nats/connect "nats://localhost:4222"))

(nats/publish conn
  {:nats.message/subject "chat.general.christian"
   :nats.message/data {:message "Hello world!"}
   :nats.message/headers {:user "Christian"}       ;; Optional
   :nats.message/reply-to "chat.general.replies"}) ;; Optional
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

### Request/response

```clj
(require '[nats.core :as nats])

(def conn (nats/connect "nats://localhost:4222"))
(def subscription (nats/subscribe conn "chat.>"))

(.start
 (Thread.
  (fn []
    (let [msg (nats/pull-message subscription 500)]
      (prn 'Request msg)
      (nats/publish conn
        {:nats.message/subject (:nats.message/reply-to msg)
         :nats.message/data {:message "Hi there, fella"}})))))

(def response (nats/request conn
                {:nats.message/subject "chat.general"
                 :nats.message/data {:message "Hello world!"}}))

(prn 'Response @response)
(nats/unsubscribe subscription)
(nats/close conn)
```

### Streams

Create a stream:

```clj
(require '[nats.core :as nats]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(stream/create-stream conn
  {:nats.stream/name "test-stream"
   :nats.stream/subjects ["test.work.>"]
   :nats.stream/allow-direct? true
   :nats.stream/retention-policy :nats.retention-policy/work-queue})
```

Publish to a stream subject:

```clj
(require '[nats.core :as nats]
         '[nats.stream :as stream])

(def conn (nats/connect "nats://localhost:4222"))

(stream/publish conn
  {:nats.message/subject "test.work.email.ed281046-938e-4096-8901-8bd6be6869ed"
   :nats.message/data {:email/to "christian@cjohansen.no"
                       :email/subject "Hello, world!"}})
```

Create a consumer:

```clj
(require '[nats.core :as nats]
         '[nats.consumer :as consumer])

(def conn (nats/connect "nats://localhost:4222"))

(consumer/create-consumer conn
  {:nats.consumer/stream-name "test-stream"
   :nats.consumer/name "test-consumer"
   :nats.consumer/durable? true
   :nats.consumer/filter-subject "test.work.>"})

;; Review its configuration
(consumer/get-consumer-info conn "test-stream" "test-consumer")
```

Consume stream messages:

```clj
(require '[nats.core :as nats]
         '[nats.consumer :as consumer])

(def conn (nats/connect "nats://localhost:4222"))

(with-open [subscription (consumer/subscribe conn "test-stream" "test-consumer")]
  (let [message (consumer/pull-message subscription 1000)] ;; Wait for up to 1000ms
    (consumer/ack conn message)
    (prn message)))
```

Review stream configuration and state:

```clj
(require '[nats.stream :as stream])

(stream/get-stream-config conn "test-stream")
(stream/get-stream-info conn "test-stream")
(stream/get-stream-state conn "test-stream")
(stream/get-mirror-info conn "test-stream")
(stream/get-consumers conn "test-stream")
```

Peek at some messages from the stream:

```clj
(stream/get-first-message conn "test-stream" "test.work.email.*")
(stream/get-last-message conn "test-stream" "test.work.email.*")
(stream/get-message conn "test-stream" 3)
(stream/get-next-message conn "test-stream" 2 "test.work.email.*")
```

Get information from the server:

```clj
(require '[nats.stream :as stream])

(stream/get-streams conn)
(stream/get-account-statistics conn)
```

### Key/value

Create a key/value store bucket:

```clj
(require '[nats.core :as nats]
         '[nats.kv :as kv])

(def conn (nats/connect "nats://localhost:4222"))

(kv/create-bucket conn
  {::kv/bucket-name "my-kv-store"
   ::kv/max-history-per-key 24})
```

Put a key/value pair and retrieve it:

```clj
(kv/put conn :my-kv-store/fruits #{"Banana" "Apple"})
(kv/get conn :my-kv-store/fruits)
;;=>
;; {:nats.kv.entry/bucket "my-kv-store"
;;  :nats.kv.entry/key "fruits"
;;  :nats.kv.entry/created-at #time/inst "2024-07-07T07:53:09.315650Z"
;;  :nats.kv.entry/operation :nats.kv-operation/put
;;  :nats.kv.entry/revision 1
;;  :nats.kv.entry/value #{"Banana" "Apple"}}

(kv/get-value conn :my-kv-store/fruits)
;;=> #{"Banana" "Apple"}
```

Delete a key:

```clj
(kv/delete conn :my-kv-store/fruits)
```

Get history on a key:

```clj
(kv/get-history conn :my-kv-store/fruits)
```

Purge all history on a key:

```clj
(kv/purge conn :my-kv-store/fruits)
```

#### Note on key/value stores

clj-nats uses headers on the underlying JetStream subject for key/value pairs in
order to automatically decode EDN data (e.g. making sure `get` retrieves Clojure
data, not opaque bytes). Using headers on key/value streams is not supported by
jnats, and there is an open discussion on whether this is officially supported.
For this reason, clj-nats' key/value implementation is somewhat experimental.

To opt out of the experimental aspect, simply do not send Clojure data directly
to `nats.kv/put` - instead, send a string, or raw bytes.

<a id="auth"></a>
## Authenticated connections

To connect using credentials, pass a map to `nats.core/connect`:

```clj
(require '[nats.core :as nats])
(import 'javax.net.ssl.SSLContext)

(nats/connect
 {::nats/server-url "nats://your.nats.server:4222"  ;; 1
  ::nats/connection-name "myapp"                    ;; 2
  ::nats/credentials-file-path "path/to/file.creds" ;; 3
  ::nats/ssl-context (SSLContext/getDefault)})      ;; 4
```

1. Your NATS server URL
2. The connection name is used for metrics. It's not strictly necessary, but
   suggested for production use.
3. The path to your credentials file. See below for alternatives.
4. When using the `nats://` protocol and a credentials file, you need to specify
   an SSL context. Alternatively you can use the `tls://` protocol and skip the
   SSL context.

### Alternatives to credential files

If you don't want to bundle a file with your app you can provide credentials (or
JWT/nkey) as strings:

```clj
(require '[nats.core :as nats])

;; Credentials
(nats/connect
 {::nats/server-url "tls://your.nats.server:4222"
  ::nats/connection-name "myapp"
  ::nats/auth-handler
  (nats/create-static-auth-handler "...")})

;; JWT + nkey
(nats/connect
 {::nats/server-url "tls://your.nats.server:4222"
  ::nats/connection-name "myapp"
  ::nats/auth-handler
  (nats/create-static-auth-handler "...jwt" "...nkey")})

;; JWT file and nkey file
(nats/connect
 {::nats/server-url "tls://your.nats.server:4222"
  ::nats/connection-name "myapp"
  ::nats/auth-handler
  (nats/create-file-auth-handler "path/to/jwt.file" "path/to/nkey.file")})
```

## Tests

To run the tests, you must run a NATS server on port 4222. The tests will
publish messages, create and delete streams and consumers, etc. Tests clean up
after themselves.

```sh
make test
```

## License

Copyright © 2024-2025 Christian Johansen Distributed under the Eclipse Public
License either version 1.0 or (at your option) any later version.
