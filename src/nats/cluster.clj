(ns ^:no-doc nats.cluster
  (:import (io.nats.client.api ClusterInfo PeerInfo)))

(defn cluster-info->map [^ClusterInfo cluster-info]
  (when cluster-info
    {::leader (.getLeader cluster-info)
     ::name (.getName cluster-info)
     ::replicas
     (for [^PeerInfo replica (.getReplicas cluster-info)]
       {:nats.peer/active (.getActive replica)
        :nats.peer/lag (.getLag replica)
        :nats.peer/name (.getName replica)
        :nats.peer/current? (.isCurrent replica)
        :nats.peer/offline? (.isOffline replica)})}))
