(ns nats.wired
  "Can we talk to NATS servers with plan Java sockets, and avoid the Java NATS
  Client?

  This namespace can establish a connection with the NATS demo server. Other
  than that, not much.

  name (\"wired\") is subject to change.

  References
  - Telnet demo: https://docs.nats.io/reference/reference-protocols/nats-protocol-demo
  - Spec: https://docs.nats.io/reference/reference-protocols/nats-protocol
  - TCP connections in Java: https://medium.com/@gaurangjotwani/creating-a-tcp-connection-between-two-servers-in-java-28fabe53deaa"
  (:refer-clojure :exclude [read-line])
  (:require [clojure.string :as str])
  (:import (java.io DataOutputStream InputStreamReader BufferedReader)
           (java.net Socket)))

(set! *warn-on-reflection* true)

(deftype Connection [^Socket socket ^BufferedReader reader
                     ^DataOutputStream data-output-stream])

(defn connect [^String host ^Integer port]
  ;; Based on https://docs.nats.io/reference/reference-protocols/nats-protocol-demo
  (let [socket (Socket. host port)
        reader (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        data-output-stream (DataOutputStream. (.getOutputStream socket))
        cleanup #(do (.close socket)
                     (.close reader)
                     (.close data-output-stream))]
    ;; First message from the NATS server is "INFO " followed by JSON.
    (let [info-message (.readLine reader)]
      (when-not (str/starts-with? info-message "INFO ")
        (cleanup)
        (throw (ex-info "Unable to connect to NATS, invalid INFO message"
                        {:info-message info-message}))))
    ;; Second, we must send "CONNECT {}".
    (.writeBytes data-output-stream "CONNECT {}\r\n")
    (.flush data-output-stream)
    (let [connect-response (.readLine reader)]
      (when-not (= connect-response "+OK")
        (cleanup)
        (throw (ex-info "Unable to connect to NATS, non-OK Connect response"
                        {:connect-response connect-response}))))
    (Connection. socket reader data-output-stream)))

(defn read-line [^Connection conn]
  (.readLine ^BufferedReader (.reader conn)))

(defn close [^Connection conn]
  (.close ^BufferedReader (.reader conn))
  (.close ^DataOutputStream (.data-output-stream conn))
  (.close ^Socket (.socket conn)))

(comment
  ;; Can connect to the demo server:
  (let [conn (connect "demo.nats.io" 4222)]
    (try
      :done
      (finally
        (close conn))))

  ;; TODO
  ;; - PUB from nats.wired, observe change with telnet
  ;; - SUB from nats.wired
  ;; - Repond to PING messages with PONG (aliveness)
  )
