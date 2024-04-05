(ns nats.cluster
  (:import (io.nats.client.api ClusterInfo PeerInfo)))

(defn cluster-info->map [^ClusterInfo cluster-info]
  {:leader (.getLeader cluster-info)
   :name (.getName cluster-info)
   :replicas (for [^PeerInfo replica (.getReplicas cluster-info)]
               {:active (.getActive replica)
                :lag (.getLag replica)
                :name (.getName replica)
                :current? (.isCurrent replica)
                :offline? (.isOffline replica)})})
