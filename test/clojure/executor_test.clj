(ns dirigiste.executor-test
  (:require
    [clojure.test :refer :all])
  (:import
    [java.util
     EnumSet]
    [java.util.concurrent
     RejectedExecutionException
     CountDownLatch
     SynchronousQueue
     TimeUnit]
    [io.aleph.dirigiste
     Executors
     Executor
     Executor$Controller
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

(defn- shutdown-task [times task-timeout-ms]
  (let [res (atom 0)]
    [res (fn [] (loop [cnt 1]
                  (swap! res inc)
                  (Thread/sleep task-timeout-ms)
                  (when (< cnt times)
                    (recur (inc cnt)))))]))

(defn- check-shutdown-after [ex task-timeout-ms]
  (let [times 3
        [res task] (shutdown-task times task-timeout-ms)]
    (.execute ex task)
    (Thread/sleep 10)
    (.shutdown ex)
    (is (= 1 @res))

    (Thread/sleep (+ 10 (* task-timeout-ms times)))
    (is (= times @res))

    (is (thrown? RejectedExecutionException
                 (.execute ex task)))
    (Thread/sleep (+ task-timeout-ms 10))
    (is (= times @res))

    (is (.isShutdown ex))
    (is (.isTerminated ex))))

(defn- check-shutdown-now-after [ex task-timeout-ms]
  (let [times 3
        [res task] (shutdown-task times task-timeout-ms)]
    (.execute ex task)
    (Thread/sleep 10)
    (is (= 1 (count (.shutdownNow ex))))
    (is (= 1 @res))

    ;; task must have been interrupted
    (Thread/sleep (+ 10 (* task-timeout-ms (inc times))))
    (is (= 1 @res))

    (is (thrown? RejectedExecutionException
                 (.execute ex task)))
    (Thread/sleep (+ task-timeout-ms 10))
    (is (= 1 @res))

    (is (.isShutdown ex))
    (is (.isTerminated ex))))

(defn- custom-fixed-executor [controller]
  (Executor. (java.util.concurrent.Executors/defaultThreadFactory)
             (java.util.concurrent.SynchronousQueue. false)
             controller 1 (EnumSet/noneOf Stats$Metric)
             10 100 TimeUnit/MILLISECONDS))

#_(deftest shutdown-custom-fixed-executor
  (let [controller (Executors/fixedController 1)]
    (check-shutdown-now-after (custom-fixed-executor controller) 100)
    (check-shutdown-after (custom-fixed-executor controller) 100)))

(deftest shutdown-manifold-fixed-executor
  (let [controller (reify Executor$Controller
                     (shouldIncrement [_ n]
                       (< n 1))
                     (adjustment [_ s]
                       (- 1 (.getNumWorkers s))))]
    (check-shutdown-now-after (custom-fixed-executor controller) 30)
    (check-shutdown-after (custom-fixed-executor controller) 30)))
