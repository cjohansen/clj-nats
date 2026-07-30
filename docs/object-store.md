# NATS Object Store

NATS Object Store lets you store and index binary large objects (BLOBs).
Official documentation: https://docs.nats.io/learn/object-store/

When do you need Object Store?
Rule of thumb:

| object size        | recommended storage machinery                                     |
|--------------------|-------------------------------------------------------------------|
| size < 10 MB       | NATS KV                                                           |
| 1 MB < size < 1 TB | NATS Object Store                                                 |
| 1 TB < size        | distributed object stores like Amazon S3 and Google Cloud Storage |

Actual numbers will vary based on how you configure and run NATS, but in short:

1. NATS KV stores small values quickly. Each value must fit in a JetStream message.
2. NATS Object Store stores larger values. Objects are split across multiple JetStream messages.
3. A NATS JetStream must still fit on one physical machine.
   Offerings like Amazon S3 and Google Cloud Storage, in contrast, have no upper limit on bucket size.

NATS Object Store has a quick list-objects operation (by using a JetStream subject to store paths).
In contrast, the S3 API does *not* guarantee quick listing.

## Quickstart

First, ensure you have NATS running with JetStream storage.

    nats-server --jetstream

Then, in your REPL,

```clojure
(require '[nats.core :as nats]
         '[nats.object-store :as object-store])

(defonce connection (nats/connect "nats://localhost:4222"))
(def bucket "demo")
(object-store/create-bucket connection {:nats.object-store/bucket-name bucket})

(object-store/put-str connection bucket "hello-world.txt" "hello, objects!")
(object-store/get-str connection bucket "hello-world.txt")
;; => "hello, objects!"
```

Congratulations, you stored your first object!

## Object metadata

put-str returns information about what happened.

```clojure
(object-store/put-str connection bucket "hello-replers.txt" "hello, REPLers!")
;; => {:nats.object/headers {},
;;     :nats.object/nuid "05BiEipN6QSjUncI8iUsZV",
;;     :nats.object/deleted? false,
;;     :nats.object/digest "SHA-256=87_n-T84JOGmr3LWtfmGvQPaCnCucVTg39q88LoniFQ=",
;;     :nats.object/modified-at #time/inst "2026-07-30T09:56:33.104834Z",
;;     :nats.object/chunks 1,
;;     :nats.object/name "hello-replers.txt",
;;     :nats.object/bucket "demo",
;;     :nats.object/size-bytes 15}
```

You can get this information back for an object,

```clojure
(object-store/get-info connection bucket "hello-replers.txt")
;; => {:nats.object/headers {},
;;     :nats.object/nuid "05BiEipN6QSjUncI8iUsZV",
;;     :nats.object/deleted? false,
;;     :nats.object/digest "SHA-256=87_n-T84JOGmr3LWtfmGvQPaCnCucVTg39q88LoniFQ=",
;;     :nats.object/modified-at #time/inst "2026-07-30T09:56:33.104676Z",
;;     :nats.object/chunks 1,
;;     :nats.object/name "hello-replers.txt",
;;     :nats.object/bucket "demo",
;;     :nats.object/size-bytes 15}
```

or list all objects.

```clojure
(object-store/list connection bucket)
;; => ({:nats.object/headers {},
;;      :nats.object/nuid "05BiEipN6QSjUncI8iUsVk",
;;      :nats.object/deleted? false,
;;      :nats.object/digest "SHA-256=qSIzFra0EhS8hfxyftR1g62hqV5soabjUDq1iC4CZuA=",
;;      :nats.object/modified-at #time/inst "2026-07-30T09:55:28.146599Z",
;;      :nats.object/chunks 1,
;;      :nats.object/name "hello-world.txt",
;;      :nats.object/bucket "demo",
;;      :nats.object/size-bytes 15}
;;     {:nats.object/headers {},
;;      :nats.object/nuid "05BiEipN6QSjUncI8iUsZV",
;;      :nats.object/deleted? false,
;;      :nats.object/digest "SHA-256=87_n-T84JOGmr3LWtfmGvQPaCnCucVTg39q88LoniFQ=",
;;      :nats.object/modified-at #time/inst "2026-07-30T09:56:33.104676Z",
;;      :nats.object/chunks 1,
;;      :nats.object/name "hello-replers.txt",
;;      :nats.object/bucket "demo",
;;      :nats.object/size-bytes 15})
```

## Binary objects

![](171_Magnolien.JPG)
*Magnolia tree. Source: [Wikimedia](https://commons.wikimedia.org/wiki/File:171_Magnolien.JPG), licensed CC BY-SA 2.5*

Files, such as this image of a beautiful tree, should be treated as bytes, not strings.
For convenience, we'll use [babashka/fs] to read the file.

[babashka/fs]: https://github.com/babashka/fs

```clojure
(require 'clojure.repl.deps)
(clojure.repl.deps/add-lib 'babashka/fs)
(require '[babashka.fs :as fs])
```

Download the image to your current directory, then

```clojure
(object-store/put-bytes connection bucket "171_Magnolien.JPG" (fs/read-all-bytes "171_Magnolien.JPG"))
;; => {:nats.object/headers {},
;;     :nats.object/nuid "05BiEipN6QSjUncI8iUtEq",
;;     :nats.object/deleted? false,
;;     :nats.object/digest "SHA-256=AHGPDVWdgwOMB-24y5T2K8oS2Z0omwfXlQVsPiGzEEM=",
;;     :nats.object/modified-at #instant "2026-07-30T10:25:42.025641Z",
;;     :nats.object/chunks 4,
;;     :nats.object/name "171_Magnolien.JPG",
;;     :nats.object/bucket "demo",
;;     :nats.object/size-bytes 479950}
```

Observe that it took more space than our hello world (479950 vs 15 bytes), and was split across multiple chunks (4 vs 1).

```clojure
(fs/write-bytes "the-tree.jpg" (object-store/get-bytes connection bucket "171_Magnolien.JPG"))

(map fs/size ["171_Magnolien.JPG" "the-tree.jpg"])
;; => (479950 479950)
```

Storing the image in Object Store did not alter its file size.
Nice!

## Watching for changes

You can get notified on every change in an object store.

```clojure
(defonce events (atom []))
(defn log! [event] (swap! events conj event))

(object-store/add-watch connection bucket ::my-watcher log!)

(object-store/put-str connection bucket "hello.txt" "hi!")
(object-store/put-str connection bucket "hello.txt" "hi there!")
(object-store/put-str connection bucket "bye.txt" "goodbye :)")

(object-store/remove-watch connection bucket ::my-watcher)

(first @events)
;; => {:nats.object/headers {},
;;     :nats.object/nuid "05BiEipN6QSjUncI8iUt3Z",
;;     :nats.object/deleted? false,
;;     :nats.object/digest "SHA-256=qSIzFra0EhS8hfxyftR1g62hqV5soabjUDq1iC4CZuA=",
;;     :nats.object/modified-at #instant "2026-07-30T10:10:03.806256Z",
;;     :nats.object/chunks 1,
;;     :nats.object/name "hello-world.txt",
;;     :nats.object/bucket "demo",
;;     :nats.object/size-bytes 15}

(count @events)
;; => 6

(last @events)
;; => {:nats.object/headers {},
;;     :nats.object/nuid "05BiEipN6QSjUncI8iUtf9",
;;     :nats.object/deleted? false,
;;     :nats.object/digest "SHA-256=I_eFlw3SHETuaiKh4Xy9Y4a06Jgz1hPTiB9-cHLLlzc=",
;;     :nats.object/modified-at #instant "2026-07-30T10:38:11.727191Z",
;;     :nats.object/chunks 1,
;;     :nats.object/name "bye.txt",
;;     :nats.object/bucket "demo",
;;     :nats.object/size-bytes 10}
```

Did you expect three events?
By default, a watcher gets events back in time, from when the bucket was created.
See the `add-watch` docstring for configuration options.

Watchers should be closed when you're done with them.
You've already seen remove-watch, which is the recommended, REPL-friendly way to remove a watcher.
If you do nothing, any open watchers are closed when the connection is closed.
But there's a third option: `clojure.core/with-open`!
Here's a small example:

```clojure
(let [events2 (atom [])
      log2 (fn [event] (swap! events2 conj event))]
  (with-open [_watcher (object-store/add-watch
                        connection bucket (gensym)
                        log2
                        {:watch-options #{:nats.object-store.watch-option/updates-only}})]
    (object-store/put-str connection bucket "hello2.txt" "hello, there!")
    (object-store/put-str connection bucket "hello2.txt" "hello, ducklings!")
    (object-store/delete connection bucket "hello2.txt")

    ;; to give the final event time to propagate, we ask for object info and
    ;; discard the result.
    (object-store/get-info connection bucket "hello2.txt"))
  (map #(select-keys % [:nats.object/name :nats.object/size-bytes :nats.object/deleted?])
       @events2))
;; => ({:nats.object/name "hello2.txt",
;;      :nats.object/size-bytes 13,
;;      :nats.object/deleted? false}
;;     {:nats.object/name "hello2.txt",
;;      :nats.object/size-bytes 17,
;;      :nats.object/deleted? false}
;;     {:nats.object/name "hello2.txt",
;;      :nats.object/size-bytes 0,
;;      :nats.object/deleted? true})
```
