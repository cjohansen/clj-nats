(ns ^:no-doc nats.cluster
  (:import (io.nats.client.api ClusterInfo PeerInfo)))

(defn cluster-info->map [^ClusterInfo cluster-info]
  (when cluster-info
    (let [cluster-name (.getName cluster-info)
          replicas (.getReplicas cluster-info)]
      (cond-> {::leader (.getLeader cluster-info)}
        cluster-name
        (assoc ::name cluster-name)

        (seq replicas)
        (assoc ::replicas
               (set (for [^PeerInfo replica replicas]
                      {:nats.peer/active (.getActive replica)
                       :nats.peer/lag (.getLag replica)
                       :nats.peer/name (.getName replica)
                       :nats.peer/current? (.isCurrent replica)
                       :nats.peer/offline? (.isOffline replica)})))))))
