(ns ^:no-doc nats.internals
  "Various helper functions used by clj-nats, not for external use."
  (:import (java.time Duration)))

(defn duration? [x]
  (instance? Duration x))

(defn ->duration ^Duration [millis-or-duration]
  (cond
    (number? millis-or-duration)
    (Duration/ofMillis millis-or-duration)

    (duration? millis-or-duration)
    millis-or-duration

    :else
    (throw (ex-info (str "Expected millis or duration, got " (type millis-or-duration))
                    {:millis-or-duration millis-or-duration}))))

(defn ->durations ^Duration/1 [millis-or-duration-s]
  (into-array Duration (map ->duration millis-or-duration-s)))

(defprotocol NatsConnectionState
  (get-state [self])
  (set-state! [self k v]))
