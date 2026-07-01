(ns user)

;; Needs alter-var-root to actually take effect for all namespaces.
;; https://clojurians.slack.com/archives/CBJ5CGE0G/p1782897028198199?thread_ts=1782894361.089249&cid=CBJ5CGE0G
(alter-var-root #'*warn-on-reflection* (constantly true))
