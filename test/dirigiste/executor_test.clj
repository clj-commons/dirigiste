(ns dirigiste.executor-test
  (:require
    [clojure.test :refer :all])
  (:import
    [java.util
     EnumSet]
    [java.util.concurrent
     CountDownLatch
     SynchronousQueue]
    [io.aleph.dirigiste
     Executors
     Executor
     Executor$Metrics
     Stats]))

(defn run-producer [^java.util.concurrent.Executor ex n interval]
  (dotimes [_ n]
    (let [latch (CountDownLatch. 1)]
      (.execute ex
        #(do (Thread/sleep interval) (.countDown latch)))
      (.await latch))))

(defn stress-executor [ex n m interval]
  (try
    (->> (range n)
      (map (fn [_] (future (run-producer ex m interval))))
      doall
      (map deref)
      doall)
    (.getStats ex)
    (finally
      (.shutdown ex))))
