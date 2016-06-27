(ns dirigiste.pool-test
  (:require
    [clojure.test :refer :all])
  (:import
    [java.util.concurrent
     TimeUnit]
    [io.aleph.dirigiste
     Pools
     Pool
     IPool$Generator
     IPool$Controller]))

(defn generator [disposed]
  (let [cnt (atom 0)]
    (reify IPool$Generator
      (generate [_ k]
        (swap! cnt inc))
      (destroy [_ k v]
        (swap! disposed conj [k v])))))

(defn controller [f]
  (reify IPool$Controller
    (shouldIncrement [_ key objects-for-key total-objects]
      true)
    (adjustment [_ key->stats]
      (f key->stats))))

(defn pool
  ([generator controller]
   (pool generator controller 1e5))
  ([generator controller max-queue-size]
   (Pool. generator controller max-queue-size 25 1e4 TimeUnit/MILLISECONDS)))

(deftest test-basic-pool-ops
  (let [disposed (atom #{})
        p (pool (generator disposed) (controller (constantly {})))]
    (try
      (is (= 1 (.acquire p :foo)))
      (is (= 2 (.acquire p :bar)))
      (.release p :foo 1)
      (is (= 1 (.acquire p :foo)))
      (is (= 3 (.acquire p :foo)))
      (.release p :bar 2)
      (.release p :foo 1)
      (is (= 2 (.acquire p :bar)))
      (.dispose p :bar 2)
      (.dispose p :foo 1)
      (is (= #{[:foo 1] [:bar 2]} @disposed))
      (finally
        (.shutdown p)))))

(defn adjustment-stats [f]
  (let [stats (promise)
        p (pool
            (generator (atom #{}))
            (controller #(do (deliver stats %) {})))]
    (try
      (f p)
      @stats
      (finally
        (.shutdown p)))))

(defn simple-generator [generate-fn]
  (reify IPool$Generator
    (generate [_ k]
      (generate-fn k))
    (destroy [_ k v])))

;; Test for: https://github.com/ztellman/dirigiste/issues/7
(deftest test-acquire-error-after-throw-in-addobject
  (let [p (pool
            (simple-generator (fn [k] (if (= :fail k)
                                        (throw (Exception. "Failed"))
                                        k)))
            (Pools/fixedController 1 1)
            1)]
    (is (= :ok (.acquire p :ok)))
    (.dispose p :ok :ok)

    (is (thrown-with-msg? Exception #"Failed"
          (.acquire p :fail)))
    (is (thrown-with-msg? Exception #"Failed"
          (.acquire p :fail)))))

(deftest test-adjustment
  (let [stats (:foo
                (adjustment-stats
                  (fn [p]
                    (dotimes [_ 1e2]
                      (dotimes [i 10]
                        (let [x (.acquire p :foo)]
                          (Thread/sleep i)
                          (.release p :foo x)))))))]
    (is (< 0 (.getTaskLatency stats 0.1) 3e6))
    (is (< 4e6 (.getTaskLatency stats 0.5) 7e6))
    (is (< 9e6 (.getTaskLatency stats 0.9) 12e6))))
