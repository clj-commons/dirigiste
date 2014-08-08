(ns dirigiste
  (:import
    [dirigiste
     DirigisteExecutorService
     DirigisteExecutorService$IExecutorService]
    [java.util
     ArrayList]
    [java.util.concurrent
     BlockingQueue
     ThreadFactory
     ExecutorService
     CountDownLatch
     CopyOnWriteArrayList
     RejectedExecutionException
     TimeUnit
     SynchronousQueue]
    [java.lang.ref
     WeakReference]
    [java.util.concurrent.atomic
     AtomicBoolean
     AtomicReference
     AtomicLong]
    [java.util.concurrent.locks
     Lock]))

(set! *warn-on-reflection* true)

(definterface IWorker
  (numCompleted [])
  (currentRunnable [])
  (shutdown [])
  (awaitTermination [timeout])
  (getThread []))

(defrecord ExecutorStats
  [^double utilization
   ^long total
   ^long completed])

(alter-meta! #'->ExecutorStats assoc :private true)

(defn- start-worker
  [^ThreadFactory thread-factory
   ^BlockingQueue q
   ^AtomicLong counter
   ^CopyOnWriteArrayList workers]
  (.incrementAndGet counter)
  (let [shutdown? (AtomicBoolean. false)
        runnable  (AtomicReference. nil)
        completed (AtomicLong. 0)
        latch     (CountDownLatch. 1)
        worker    (promise)
        thread    (.newThread thread-factory
                    (fn []
                      (while (not (.get shutdown?))
                        (let [^Runnable r (.take q)]
                          (try
                            (.set runnable r)
                            (.run r)
                            (catch Throwable e
                              (.printStackTrace e))
                            (finally
                              (.incrementAndGet completed)
                              (.set runnable nil)))))
                      (.remove workers @worker)
                      (.decrementAndGet counter)
                      (.countDown latch)))]

    (.start thread)

    (deliver worker
      (reify IWorker
        (numCompleted [_] (.getAndSet completed 0))
        (currentRunnable [_] (.get runnable))
        (shutdown [_] (.compareAndSet shutdown? false true))
        (getThread [_] thread)
        (awaitTermination [_ timeout] (.await latch timeout TimeUnit/MILLISECONDS))))

    (.add workers @worker)))

(defn- control-loop
  [max-thread-count
   thread-factory
   queue
   controller
   ^WeakReference workers
   counter
   ^AtomicBoolean shutdown?
   sample-period
   samples-per-control]
  (fn []
    (loop
      [next-sample (+ (System/currentTimeMillis) sample-period)
       num-samples 0
       active-count 0
       thread-count 0
       tasks 0]

      (when-let [^CopyOnWriteArrayList workers (and (not (.get shutdown?)) (.get workers))]
        (Thread/sleep (Math/max 0 (long (- next-sample (System/currentTimeMillis)))))

        (if-let [[active-count thread-count tasks]
                 (loop
                   [s (-> workers .iterator iterator-seq)
                    active-count active-count
                    thread-count thread-count
                    tasks tasks]

                   (if (empty? s)
                     (if (= num-samples samples-per-control)

                       (let [stats (->ExecutorStats
                                     (if (zero? thread-count)
                                       0.0
                                       (double (/ active-count thread-count)))
                                     (.size workers)
                                     tasks)
                             adjustment (long (controller stats))]
                         (if (neg? adjustment)
                           (loop [remaining adjustment
                                  s (-> workers .iterator iterator-seq)]
                             (when-not (or (zero? remaining) (empty? workers))
                               (let [^IWorker w (first s)]
                                 (if (.shutdown w)
                                   (recur (dec remaining) (rest s))
                                   (recur remaining (rest s))))))
                           (dotimes [_ (Math/min (long (- max-thread-count (.size workers))) adjustment)]
                             (start-worker thread-factory queue counter workers)))
                         nil)

                       [active-count thread-count tasks])
                     (let [^IWorker w (first s)]
                       (recur
                         (rest s)
                         (if (.currentRunnable w) (inc active-count) active-count)
                         (inc thread-count)
                         (+ tasks (.numCompleted w))))))]
          (recur
            (+ next-sample sample-period)
            (inc num-samples)
            active-count
            thread-count
            tasks)
          (recur (+ next-sample sample-period) 0 0 0 0))))))

(defn executor-service
  ([max-thread-count
    sample-frequency
    control-frequency
    controller]
     (let [cnt (atom 0)]
       (executor-service
         max-thread-count
         sample-frequency
         control-frequency
         controller
         (reify ThreadFactory
           (newThread [_ r]
             (Thread. r (str "dirigiste-worker-" (swap! cnt inc)))))
         (SynchronousQueue.))))
  ([max-thread-count
    sample-frequency
    control-frequency
    controller
    ^ThreadFactory thread-factory
    ^BlockingQueue queue]
     (let [max-thread-count (long max-thread-count)
           workers (CopyOnWriteArrayList.)
           shutdown? (AtomicBoolean. false)
           counter (AtomicLong. 0)]

       (let [sample-period (long (/ 1000 sample-frequency))
             samples-per-control (long (/ sample-frequency control-frequency))]
         (.start
           (Thread.
             ^Runnable
             (control-loop
               max-thread-count
               thread-factory
               queue
               controller
               (WeakReference. workers)
               counter
               shutdown?
               sample-period
               samples-per-control)
             "dirigiste-pool-control-thread")))

       (DirigisteExecutorService.
         (reify DirigisteExecutorService$IExecutorService

           (awaitTermination [_ timeout]
             (let [start (System/currentTimeMillis)]
               (loop [workers (seq workers)]
                 (if (empty? workers)
                   true
                   (let [interval (- timeout (- (System/currentTimeMillis) start))
                         ^IWorker w (first workers)]
                     (if (.awaitTermination w interval)
                       (recur (rest workers))
                       false))))))

           (execute [_ runnable]
             (when-not (.offer queue runnable)
               (if (>= (.size workers) max-thread-count)
                 (throw (RejectedExecutionException.))
                 (do
                   (start-worker thread-factory queue counter workers)
                   (.put queue runnable)))))

           (isShutdown [_]
             (.get shutdown?))

           (isTerminated [_]
             (and (.get shutdown?) (zero? (.get counter))))

           (shutdown [_]
             (.set shutdown? true)
             (doseq [^IWorker w workers]
               (.shutdown w)))

           (shutdownNow [_]
             (let [lst (ArrayList.)
                   pending (.drainTo queue lst)]
               (doseq [^IWorker w workers]
                 (when-let [r (.currentRunnable w)]
                   (.add lst w)))
               lst)))))))
