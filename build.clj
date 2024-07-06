(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "src")
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn compile [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "11"]}))
