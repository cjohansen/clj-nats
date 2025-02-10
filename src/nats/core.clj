(ns nats.core
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [nats.message :as message])
  (:import (io.nats.client Connection
                           ConnectionListener
                           ConnectionListener$Events
                           ErrorListener
                           Nats
                           Options
                           Options$Builder
                           ReconnectDelayHandler
                           StatisticsCollector
                           Subscription
                           TimeTraceLogger)
           (java.time Duration ZoneId)))

(def ^:no-doc connections (atom {}))

(def ^:no-doc default-tz
  "The Java SDK uses ZonedDateTime for every instant and defaults the time zone to
   GMT. All the NATS times are instants in time, so Instant is the appropriate
   representation for them - no need to wrap them all in a timezone. This default
   timezone is here only to convert incoming Instants to the ZonedDateTime the Java
   SDK expects."
  (ZoneId/of "GMT"))

(defn- covers? [haystacks needles]
  (if (< (count needles) (count haystacks))
    false
    (loop [[haystack & haystacks] haystacks
           [needle & needles] needles]
      (cond
        (and (empty? haystack) (empty? needle))
        true

        (= haystack needle)
        (recur haystacks needles)

        (and (= "*" haystack) needle)
        (recur haystacks needles)

        (and (= ">" haystack) needle)
        true))))

(defn ^:no-doc covers-subject? [patterns subject]
  (let [s-pieces (str/split subject #"\.")]
    (boolean (some #(covers? (str/split % #"\.") s-pieces) patterns))))

(defn ^:export create-file-auth-handler
  "Creates an `io.nats.client.AuthHandler` for a credentials file, or a jwt-file
  and a nkey-file. The result can be passed as `:nats.core/auth-handler` in
  `nats.core/connect`."
  ([credentials-file-path]
   (Nats/credentials credentials-file-path))
  ([jwt-file-path nkey-file-path]
   (Nats/credentials jwt-file-path nkey-file-path)))

(defn- strbytes [s]
  (if (string? s)
    (.getBytes ^String s)
    s))

(defn ^:export create-static-auth-handler
  "Creates an `io.nats.client.AuthHandler` for a credential string, or a pair of JWT and nkey strings.
  The result can be passed as `:nats.core/auth-handler` in `nats.core/connect`."
  ([credentials]
   (Nats/staticCredentials (strbytes credentials)))
  ([jwt nkey]
   (Nats/staticCredentials (strbytes jwt) (strbytes nkey))))

(defn ^:export create-error-listener
  "Create an `io.nats.client.ErrorListener` instance. Takes the following
  functions:

  - `:error-occurred` `(fn [conn error])`
  - `:exception-occurred` `(fn [conn exception])`
  - `:flow-control-processed`  `(fn [conn subscription subject source])`
  - `:heartbeat-alarm`  `(fn [conn subscription last-stream-seq last-consumer-seq])`
  - `:message-discarded`  `(fn [conn message])`
  - `:pull-status-error`  `(fn [conn subscription status])`
  - `:pull-status-warning`  `(fn [conn subscription status])`
  - `:slow-consumer-detected`  `(fn [conn consumer])`
  - `:socket-write-timeout`  `(fn [conn])`
  - `:supply-message`  `(fn [label conn consumer sub pairs])`
  - `:unhandled-status`  `(fn [conn subscription status])`"
  [{:keys [error-occurred
           exception-occurred
           flow-control-processed
           heartbeat-alarm
           message-discarded
           pull-status-error
           pull-status-warning
           slow-consumer-detected
           socket-write-timeout
           supply-message
           unhandled-status]}]
  (reify ErrorListener
    (errorOccurred [_this conn error]
      (when (ifn? error-occurred)
        (error-occurred (get @connections conn) error)))

    (exceptionOccurred [_this conn exception]
      (when (ifn? exception-occurred)
        (exception-occurred (get @connections conn) exception)))

    (flowControlProcessed [_this conn subscription subject source]
      (when (ifn? flow-control-processed)
        (flow-control-processed (get @connections conn) subscription subject source)))

    (heartbeatAlarm [_this conn subscription last-stream-seq last-consumer-seq]
      (when (ifn? heartbeat-alarm)
        (heartbeat-alarm (get @connections conn) subscription last-stream-seq last-consumer-seq)))

    (messageDiscarded [_this conn message]
      (when (ifn? message-discarded)
        (message-discarded (get @connections conn) (message/message->map message))))

    (pullStatusError [_this conn subscription status]
      (when (ifn? pull-status-error)
        (pull-status-error (get @connections conn) subscription (message/status->map status))))

    (pullStatusWarning [_this conn subscription status]
      (when (ifn? pull-status-warning)
        (pull-status-warning (get @connections conn) subscription (message/status->map status))))

    (slowConsumerDetected [_this conn consumer]
      (when (ifn? slow-consumer-detected)
        (slow-consumer-detected (get @connections conn) consumer)))

    (socketWriteTimeout [_this conn]
      (when (ifn? socket-write-timeout)
        (socket-write-timeout (get @connections conn))))

    (supplyMessage [_this label conn consumer sub pairs]
      (when (ifn? supply-message)
        (supply-message label conn consumer sub pairs)))

    (unhandledStatus [_this conn subscription status]
      (when (ifn? unhandled-status)
        (unhandled-status (get @connections conn) subscription (message/status->map status))))))

(defn ^:export create-statistics-collector
  "Create a `io.nats.client.StatisticsCollector` instance. Takes a map of
  functions:

  - `:decrement-outstanding-requests` `(fn [])`
  - `:increment-dropped-count` `(fn [])`
  - `:increment-duplicate-replies-received` `(fn [])`
  - `:increment-err-count` `(fn [])`
  - `:increment-exception-count` `(fn [])`
  - `:increment-flush-counter` `(fn [])`
  - `:increment-in-bytes` `(fn [])`
  - `:increment-in-msgs` `(fn [])`
  - `:increment-ok-count` `(fn [])`
  - `:increment-orphan-replies-received` `(fn [])`
  - `:increment-out-bytes` `(fn [byte-count])`
  - `:increment-out-msgs` `(fn [])`
  - `:increment-outstanding-requests` `(fn [])`
  - `:increment-ping-count` `(fn [])`
  - `:increment-reconnects` `(fn [])`
  - `:increment-replies-received` `(fn [])`
  - `:increment-requests-sent` `(fn [])`
  - `:register-read` `(fn [byte-count])`
  - `:register-write` `(fn [byte-count])`
  - `:set-advanced-tracking` `(fn [track-advance])`"
  [{:keys [decrement-outstanding-requests
           increment-dropped-count
           increment-duplicate-replies-received
           increment-err-count
           increment-exception-count
           increment-flush-counter
           increment-in-bytes
           increment-in-msgs
           increment-ok-count
           increment-orphan-replies-received
           increment-out-bytes
           increment-out-msgs
           increment-outstanding-requests
           increment-ping-count
           increment-reconnects
           increment-replies-received
           increment-requests-sent
           register-read
           register-write
           set-advanced-tracking]}]
  (reify StatisticsCollector
    (decrementOutstandingRequests [_this]
      (when (ifn? decrement-outstanding-requests)
        (decrement-outstanding-requests)))

    (incrementDroppedCount [_this]
      (when (ifn? increment-dropped-count)
        (increment-dropped-count)))

    (incrementDuplicateRepliesReceived [_this]
      (when (ifn? increment-duplicate-replies-received)
        (increment-duplicate-replies-received)))

    (incrementErrCount [_this]
      (when (ifn? increment-err-count)
        (increment-err-count)))

    (incrementExceptionCount [_this]
      (when (ifn? increment-exception-count)
        (increment-exception-count)))

    (incrementFlushCounter [_this]
      (when (ifn? increment-flush-counter)
        (increment-flush-counter)))

    (incrementInBytes [_this bs]
      (when (ifn? increment-in-bytes)
        (increment-in-bytes bs)))

    (incrementInMsgs [_this]
      (when (ifn? increment-in-msgs)
        (increment-in-msgs)))

    (incrementOkCount [_this]
      (when (ifn? increment-ok-count)
        (increment-ok-count)))

    (incrementOrphanRepliesReceived [_this]
      (when (ifn? increment-orphan-replies-received)
        (increment-orphan-replies-received)))

    (incrementOutBytes [_this byte-count]
      (when (ifn? increment-out-bytes)
        (increment-out-bytes byte-count)))

    (incrementOutMsgs [_this]
      (when (ifn? increment-out-msgs)
        (increment-out-msgs)))

    (incrementOutstandingRequests [_this]
      (when (ifn? increment-outstanding-requests)
        (increment-outstanding-requests)))

    (incrementPingCount [_this]
      (when (ifn? increment-ping-count)
        (increment-ping-count)))

    (incrementReconnects [_this]
      (when (ifn? increment-reconnects)
        (increment-reconnects)))

    (incrementRepliesReceived [_this]
      (when (ifn? increment-replies-received)
        (increment-replies-received)))

    (incrementRequestsSent [_this]
      (when (ifn? increment-requests-sent)
        (increment-requests-sent)))

    (registerRead [_this byte-count]
      (when (ifn? register-read)
        (register-read byte-count)))

    (registerWrite [_this byte-count]
      (when (ifn? register-write)
        (register-write byte-count)))

    (setAdvancedTracking [_this track-advance]
      (when (ifn? set-advanced-tracking)
        (set-advanced-tracking track-advance)))))

(def connection-events
  "Available events:

   - `:nats.connection.event/closed`
   - `:nats.connection.event/connected`
   - `:nats.connection.event/disconnected`
   - `:nats.connection.event/discovered-servers`
   - `:nats.connection.event/lame-duck`
   - `:nats.connection.event/reconnected`
   - `:nats.connection.event/resubscribed`"
  {:nats.connection.event/closed ConnectionListener$Events/CLOSED
   :nats.connection.event/connected ConnectionListener$Events/CONNECTED
   :nats.connection.event/disconnected ConnectionListener$Events/DISCONNECTED
   :nats.connection.event/discovered-servers ConnectionListener$Events/DISCOVERED_SERVERS
   :nats.connection.event/lame-duck ConnectionListener$Events/LAME_DUCK
   :nats.connection.event/reconnected ConnectionListener$Events/RECONNECTED
   :nats.connection.event/resubscribed ConnectionListener$Events/RESUBSCRIBED})

(def ^:no-doc connection-event->k (set/map-invert connection-events))

(defn build-options ^Options
  [{::keys [auth-handler ;; AuthHandler
            buffer-size
            client-side-limit-checks
            connection-listener ;; fn
            connection-name
            connection-timeout ;; Duration / ms
            credentials
            credentials-file-path
            data-port-type
            discard-messages-when-outgoing-queue-full?
            error-listener ;; create-error-listener
            executor-service ;; java.util.concurrent.ExecutorService
            get-wait-time ;; fn https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/ReconnectDelayHandler.html#getWaitTime-long-
            http-request-interceptor ;; https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/Options.Builder.html#httpRequestInterceptor-java.util.function.Consumer-
            http-request-interceptors ;; https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/Options.Builder.html#httpRequestInterceptors-java.util.Collection-
            ignore-discovered-servers?
            inbox-prefix
            jwt
            jwt-file-path
            keystore-password ;; char[]
            keystore-path
            max-control-line
            max-messages-in-outgoing-queue
            max-pings-out
            max-reconnects
            nkey
            nkey-file-path
            no-echo?
            no-headers?
            no-no-responders?
            no-randomize?
            no-reconnect?
            no-resolve-hostnames?
            old-request-style?
            open-tls?
            pedantic?
            ping-interval ;; Duration / ms
            reconnect-buffer-size
            ^java.time.Duration reconnect-jitter
            ^java.time.Duration reconnect-jitter-tls
            ^java.time.Duration reconnect-wait
            report-no-responders?
            ^java.time.Duration request-cleanup-interval
            secure?
            server-pool ;; https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/ServerPool.html
            server-url
            server-urls
            ^javax.net.ssl.SSLContext ssl-context
            socket-so-linger ;; "SO LINGER" in seconds
            socket-write-timeout ;; Duration / ms
            statistics-collector
            utf8-subjects?
            tls-algorithm
            tls-first?
            ^chars token ;; char[]
            trace-connection?
            ^chars truststore-password ;; char[]
            truststore-path
            advanced-stats?
            use-dispatcher-with-executor?
            user-name ;; char[] / String
            password ;; char[] / String
            time-trace
            use-timeout-exception?
            verbose?]}]
  (cond-> ^Options$Builder (Options/builder)
    auth-handler (.authHandler auth-handler)
    (and jwt nkey) (.authHandler (create-static-auth-handler jwt nkey))
    credentials (.authHandler (create-static-auth-handler credentials))
    (and jwt-file-path nkey-file-path) (.authHandler (Nats/credentials jwt-file-path nkey-file-path))
    buffer-size (.bufferSize buffer-size)
    client-side-limit-checks (.clientSideLimitChecks client-side-limit-checks)
    connection-listener (.connectionListener
                         (reify ConnectionListener
                           (connectionEvent [_this conn event]
                             (connection-listener (get @connections conn) (connection-event->k event)))))
    connection-name (.connectionName connection-name)
    (int? connection-timeout) (.connectionTimeout ^long connection-timeout)
    (instance? java.time.Duration connection-timeout) (.connectionTimeout ^java.time.Duration connection-timeout)
    credentials-file-path (.credentialPath credentials-file-path)
    data-port-type (.dataPortType data-port-type)
    discard-messages-when-outgoing-queue-full? (.discardMessagesWhenOutgoingQueueFull)
    ;; DispatcherFactory is strongly marked as "purely internal", so the builder
    ;; method .dispatcherFactory isn't exposed, for now anyway.
    error-listener (.errorListener error-listener)
    executor-service (.executor executor-service)
    http-request-interceptor (.httpRequestInterceptor http-request-interceptor)
    http-request-interceptors (.httpRequestInterceptors http-request-interceptors)
    ignore-discovered-servers? (.ignoreDiscoveredServers)
    inbox-prefix (.inboxPrefix inbox-prefix)
    keystore-password (.keystorePassword keystore-password)
    keystore-path (.keystorePath keystore-path)
    max-control-line (.maxControlLine max-control-line)
    max-messages-in-outgoing-queue (.maxMessagesInOutgoingQueue max-messages-in-outgoing-queue)
    max-pings-out (.maxPingsOut max-pings-out)
    max-reconnects (.maxReconnects max-reconnects)
    no-echo? (.noEcho)
    no-headers? (.noHeaders)
    no-no-responders? (.noNoResponders)
    no-randomize? (.noRandomize)
    no-reconnect? (.noReconnect)
    no-resolve-hostnames? (.noResolveHostnames)
    old-request-style? (.oldRequestStyle)
    open-tls? (.opentls)
    pedantic? (.pedantic)
    ping-interval (.pingInterval ping-interval)
    ;; It's not at all clear what the .properties method on the builder is for.
    ;; It might be an alternative way to initialize the "Options" class - if so,
    ;; it is unlikely to be useful to Clojure programmers. Until I can figure
    ;; out what it does and if it's relevant it is not exposed.
    reconnect-buffer-size (.reconnectBufferSize reconnect-buffer-size)
    get-wait-time (.reconnectDelayHandler
                   (reify ReconnectDelayHandler
                     (getWaitTime [_this total-tries]
                       (get-wait-time total-tries))))
    reconnect-jitter (.reconnectJitter reconnect-jitter)
    reconnect-jitter-tls (.reconnectJitterTls reconnect-jitter-tls)
    reconnect-wait (.reconnectWait reconnect-wait)
    report-no-responders? (.reportNoResponders)
    request-cleanup-interval (.requestCleanupInterval request-cleanup-interval)
    secure? (.secure)
    server-url (.server ^String server-url)
    server-pool (.serverPool ^io.nats.client.ServerPool server-pool)
    server-urls (.servers server-urls)
    socket-so-linger (.socketSoLinger ^int socket-so-linger)
    (int? socket-write-timeout) (.socketWriteTimeout ^int socket-write-timeout)
    (instance? java.time.Duration socket-write-timeout) (.socketWriteTimeout ^java.time.Duration socket-write-timeout)
    ssl-context (.sslContext ssl-context)
    ;; Because the SSLContextFactory interface is in the "impl" namespace, we'll
    ;; leave .sslContextFactory unexposed until someone complains.
    statistics-collector (.statisticsCollector statistics-collector)
    utf8-subjects? (.supportUTF8Subjects)
    time-trace (.timeTraceLogger
                (reify TimeTraceLogger
                  (trace [_this fmt args]
                    (apply time-trace fmt args))))
    tls-algorithm (.tlsAlgorithm tls-algorithm)
    tls-first? (.tlsFirst)
    token (.token token)
    trace-connection? (.traceConnection)
    truststore-password (.truststorePassword truststore-password)
    truststore-path (.truststorePath truststore-path)
    advanced-stats? (.turnOnAdvancedStats)
    use-dispatcher-with-executor? (.useDispatcherWithExecutor)
    (and user-name password) (.userInfo ^String user-name ^String password)
    use-timeout-exception? (.useTimeoutException)
    verbose? (.verbose)
    :always (.build)))

(defn ^:export connect
  "Connect to the NATS server. Optionally configure jet stream and key/value
  management, or use `nats.stream/configure` and `nats.kv/configure`
  respectively later.

  `server-url-or-options` is either the NATS server URL as a string, or a map
  of the following keys:

  - `:nats.core/auth-handler` a `AuthHandler` instance, see `create-file-auth-handler` and `create-static-auth-handler`.
  - `:nats.core/buffer-size`
  - `:nats.core/client-side-limit-checks`
  - `:nats.core/connection-listener` A function that receives connection events.
                                     Will be passed the clj-nats connection and an event keyword.
  - `:nats.core/connection-name`
  - `:nats.core/connection-timeout` java.time.Duration or number of milliseconds
  - `:nats.core/credentials`
  - `:nats.core/credentials-file-path`
  - `:nats.core/data-port-type`
  - `:nats.core/discard-messages-when-outgoing-queue-full?`
  - `:nats.core/error-listener` See `create-error-listener`
  - `:nats.core/executor-service` A `java.util.concurrent.ExecutorService` instance
  - `:nats.core/get-wait-time` A function that will be called with one argument, the number of connection
                               attempts, and that returns a `java.time.Duration` dictating how long to
                               wait before attempting another reconnect.
  - `:nats.core/http-request-interceptor` See https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/Options.Builder.html#httpRequestInterceptor-java.util.function.Consumer-
  - `:nats.core/http-request-interceptors` See https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/Options.Builder.html#httpRequestInterceptors-java.util.Collection-
  - `:nats.core/ignore-discovered-servers?`
  - `:nats.core/inbox-prefix`
  - `:nats.core/jwt`
  - `:nats.core/jwt-file-path`
  - `:nats.core/keystore-password` A character array
  - `:nats.core/keystore-path`
  - `:nats.core/max-control-line`
  - `:nats.core/max-messages-in-outgoing-queue`
  - `:nats.core/max-pings-out`
  - `:nats.core/max-reconnects`
  - `:nats.core/nkey`
  - `:nats.core/nkey-file-path`
  - `:nats.core/no-echo?`
  - `:nats.core/no-headers?`
  - `:nats.core/no-no-responders?`
  - `:nats.core/no-randomize?`
  - `:nats.core/no-reconnect?`
  - `:nats.core/no-resolve-hostnames?`
  - `:nats.core/old-request-style?`
  - `:nats.core/open-tls?`
  - `:nats.core/pedantic?`
  - `:nats.core/ping-interval` A `java.time.Duration` or a number of milliseconds
  - `:nats.core/reconnect-buffer-size`
  - `:nats.core/reconnect-jitter` `java.time.Duration`
  - `:nats.core/reconnect-jitter-tls` ;; `java.time.Duration`
  - `:nats.core/reconnect-wait` ;; `java.time.Duration`
  - `:nats.core/report-no-responders?`
  - `:nats.core/request-cleanup-interval` ;; `java.time.Duration`
  - `:nats.core/secure?`
  - `:nats.core/server-pool` See https://javadoc.io/static/io.nats/jnats/2.19.0/io/nats/client/ServerPool.html
  - `:nats.core/server-url`
  - `:nats.core/server-urls`
  - `:nats.core/ssl-context` A `javax.net.ssl.SSLContext`
  - `:nats.core/socket-so-linger` SO LINGER in seconds
  - `:nats.core/socket-write-timeout` A `java.time.Duration` or a number of milliseconds
  - `:nats.core/statistics-collector` See `create-statistics-collector`
  - `:nats.core/utf8-subjects?`
  - `:nats.core/tls-algorithm`
  - `:nats.core/tls-first?`
  - `:nats.core/token` A character array
  - `:nats.core/trace-connection?`
  - `:nats.core/truststore-password` A character array
  - `:nats.core/truststore-path`
  - `:nats.core/advanced-stats?`
  - `:nats.core/use-dispatcher-with-executor?`
  - `:nats.core/user-name` Character array or String. Must be same type as `:password`
  - `:nats.core/password` Character array or String. Must be same type as `:user-name`
  - `:nats.core/time-trace`
  - `:nats.core/use-timeout-exception?`
  - `:nats.core/verbose?`"
  [server-url-or-options & [{:keys [jet-stream-options key-value-options]}]]
  (let [conn (if (string? server-url-or-options)
               (Nats/connect ^String server-url-or-options)
               (Nats/connect (build-options server-url-or-options)))
        clj-conn (atom {:conn conn
                        :jet-stream-options jet-stream-options
                        :key-value-options key-value-options})]
    (swap! connections assoc conn clj-conn)
    clj-conn))

(defn ^:no-doc get-connection ^Connection [conn]
  (:conn @conn))

(defn ^:export close [conn]
  (let [jconn (get-connection conn)]
    (swap! connections dissoc jconn)
    (.close jconn)))

(defn ^{:style/indent 1 :export true} publish
  "Publish a message. Performs no publish acking; do not use for publishing to a
  JetStream subject, instead use `nats.stream/publish`.

  `message` is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.
  - `:nats.message/reply-to` - An optional reply-to subject."
  [conn message]
  (assert (not (nil? (::message/subject message))) "Can't publish without data")
  (assert (not (nil? (::message/data message))) "Can't publish nil data")
  (->> (message/build-message message)
       (.publish (get-connection conn))
       message/publish-ack->map))

(defn ^{:style/indent 1 :export true} subscribe
  "Subscribe to non-stream subject. For JetStream subjects, instead use
  `nats.stream/subscribe`. Pull messages with `nats.core/pull-message`."
  [conn subject & [queue-name]]
  (if queue-name
    (.subscribe (get-connection conn) subject queue-name)
    (.subscribe (get-connection conn) subject)))

(defn ^:export pull-message [^Subscription subscription timeout]
  (some-> (if (int? timeout)
            (.nextMessage subscription ^int timeout)
            (.nextMessage subscription ^java.time.Duration timeout))
          message/message->map))

(defn ^:export unsubscribe [^Subscription subscription]
  (.unsubscribe subscription)
  nil)

(defn ^{:style/indent 1 :export true} request
  "Make a request and wait for the response. Returns a future that resolves with
  the response.

  `message` is a map of:

  - `:nats.message/subject` - The subject to publish to
  - `:nats.message/data` - The message data. Can be any Clojure value
  - `:nats.message/headers` - An optional map of string keys to string (or
  collection of string) values to set as meta-data on the message.

  In request/response, `:nats.message/reply-to` is reserved for the server.

  `timeout` is optional, and either a number of milliseconds to wait, or a
  `java.time.Duration`."
  ([conn message]
   (assert (not (nil? (::message/subject message))) "Can't make request without data")
   (assert (not (nil? (::message/data message))) "Can't make request nil data")
   (future
     (->> (message/build-message (dissoc message :nats.message/reply-to))
          (.request (get-connection conn))
          deref
          message/message->map)))
  ([conn message timeout]
   (assert (not (nil? (::message/subject message))) "Can't make request without data")
   (assert (not (nil? (::message/data message))) "Can't make request nil data")
   (assert (or (number? timeout) (instance? Duration timeout)) "timeout should be millis (number) or Duration")
   (future
     (->> (.requestWithTimeout
           (get-connection conn)
           (message/build-message (dissoc message :nats.message/reply-to))
           (cond-> timeout
             (number? timeout) Duration/ofMillis))
          deref
          message/message->map))))
