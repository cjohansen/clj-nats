(ns build
  (:require [clojure.tools.build.api :as b]))

#_(def lib 'clj-nats/jnats)
;;(def version (format "1.2.%s" (b/git-count-revs nil)))
(def class-dir "src")
;;(def jar-file (format "target/%s-%s.jar" (name lib) version))

;; delay to defer side effects (artifact downloads)
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "11"]}))

(defn jar [_]
  (compile nil)
  #_(b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  #_(b/jar {:class-dir class-dir
          :jar-file jar-file}))
