(ns dirigiste.executor-test
  (:require
    [clojure.test :refer :all])
  (:import
    [java.util
     EnumSet]
    [java.util.concurrent
     CountDownLatch
     SynchronousQueue
     TimeUnit]
    [io.aleph.dirigiste
     Executors
     Executor
     Stats$Metric]))

(defn run-producer [^java.util.concurrent.Executor ex n interval]
  (dotimes [_ n]
    (let [latch (CountDownLatch. 1)]
      (try

        (.execute ex
          #(do (Thread/sleep interval) (.countDown latch)))
        (.await latch)

        (catch Throwable e
          )))))

(defn stress-executor [ex n m interval]
  (is
    (->> (range n)
      (map (fn [_] (future (run-producer ex m interval))))
      doall
      (map #(deref % 3e5 ::timeout))
      doall
      (remove nil?)
      empty?))
  (let [s (.getStats ex)]
    (prn (.getUtilization s 0.5) (.getUtilization s 0.9) (.getUtilization s 0.99))
    s))

(defn run-executor-test [pause]
  (let [ex (Executor.
             (java.util.concurrent.Executors/defaultThreadFactory)
             (SynchronousQueue. false)
             (Executors/utilizationController 0.9 64)
             1
             (EnumSet/allOf Stats$Metric)
             10
             1000
             TimeUnit/MILLISECONDS)]
    (try
      (dotimes [_ 1]
        (is (< 30 (.getNumWorkers (stress-executor ex 32 1e5 pause)) 40))
        (is (< 15 (.getNumWorkers (stress-executor ex 16 1e5 pause)) 20))
        (is (< 5 (.getNumWorkers (stress-executor ex 8 1e5 pause)) 15))
        (Thread/sleep (* 1000 10))
        (is (= 1 (-> ex .getStats .getNumWorkers))))
      (finally
        (.shutdown ex)))))

(deftest test-executor
  (run-executor-test 0)
  #_(run-executor-test 1)
  #_(run-executor-test 10))

(deftest ^:stress test-utilization-metric
  )
