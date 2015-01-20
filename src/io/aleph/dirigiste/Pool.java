package io.aleph.dirigiste;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

public class Pool<K,V> {

    // interfaces
    public interface Controller<K> {

        /**
         * @param key  the key which requires a new object
         * @param objectsForKey  the number of currently existing objects for 'key'
         * @param totalObjects  the total number of objects across every key
         * @return 'true' if a new object under 'key' should be created, false otherwise
         */
        boolean shouldIncrement(K key, int objectsForKey, int totalObjects);

        /**
         * @param stats  a map of key onto stats for that key
         * @return a map of key onto how many objects should be created (if positive) or disposed (if negative)
         */
        Map<K,Integer> adjustment(Map<K,Stats> stats);
    }

    public interface Generator<K,V> {
        /**
         * Creates a new instance of the pooled object, which must be
         * non-null, and non-equal to all other generated objects.
         *
         * @param key  the key for which the object is being generated.
         */
        V generate(K key) throws Exception;

        /**
         * Disposes of the generated value.  Should be idempotent.
         *
         * @param val  an object which was previously created via 'generate'.
         */
        void destroy(K key, V val);
    }

    public interface AcquireCallback<V> {

        /**
         * A callback that returns a pooled object.
         */
        void handleObject(V obj);
    }

    // pooled object queue
    class Queue {

        private boolean _isShutdown = false;

        private final Deque<AcquireCallback<V>> _takes;
        private final Deque<V> _puts = new LinkedBlockingDeque();
        private final K _key;

        final AtomicLong incoming = new AtomicLong(0);
        final AtomicLong completed = new AtomicLong(0);
        final AtomicLong rejected = new AtomicLong(0);
        final AtomicInteger objects = new AtomicInteger(0);

        public Queue(K key, int queueSize) {
            _key = key;
            _takes = new LinkedBlockingDeque(queueSize);
        }

        public int getQueueLength() {
            return _takes.size();
        }

        public void release(V obj) {
            completed.incrementAndGet();
            put(obj);
        }

        public void destroy(V obj) {
            try {
                _generator.destroy(_key, obj);
            } finally {
                _numObjects.decrementAndGet();
            }
        }

        public void shutdown() {
            _lock.lock();

            int n = objects.get();
            for (int i = 0; i < n; i++) {
                drop();
            }

            _isShutdown = true;

            _lock.unlock();
        }

        public void drop() {

            _lock.lock();

            int n;
            while (true) {
                n = objects.get();

                // if we're already at zero, or at one with more work to go
                // it's a no-op
                if (n <= 0 || (n == 1 && getQueueLength() > 0)) {
                    _lock.unlock();
                    return;
                }
                if (objects.compareAndSet(n, n-1)) {
                    break;
                }
            }

            try {
                take(new AcquireCallback<V>() {
                        public void handleObject(V obj) {
                            destroy(obj);
                        }
                    }, true);
            } catch (RejectedExecutionException e) {
                throw new RuntimeException(e);
            } finally {
                _lock.unlock();
            }
        }

        private void put(V obj) {
            _lock.lock();

            if (_isShutdown) {
                _lock.unlock();
                throw new IllegalStateException("already shutdown");
            }

            if (_destroyedObjects.contains(obj)) {
                objects.decrementAndGet();
                _lock.unlock();
                destroy(obj);
                return;
            }

            AcquireCallback<V> c = _takes.poll();
            if (c != null) {
                _lock.unlock();
                c.handleObject(obj);
            } else {
                _puts.add(obj);
                _lock.unlock();
            }
        }

        public int cleanup() {
            _lock.lock();

            List<V> live = new ArrayList<V>();
            List<V> dead = new ArrayList<V>();
            V obj = _puts.poll();
            while (obj != null) {
                if (!_destroyedObjects.contains(obj)) {
                    live.add(obj);
                } else {
                    dead.add(obj);
                    _destroyedObjects.remove(obj);
                    objects.decrementAndGet();
                }
                obj = _puts.poll();
            }

            int numObjects = objects.get();

            for (V o : live) {
                _puts.add(o);
            }

            _lock.unlock();

            for (V o : dead) {
                destroy(o);
            }

            return numObjects;
        }

        public boolean take(AcquireCallback<V> c, boolean skipToFront) throws RejectedExecutionException {
            incoming.incrementAndGet();
            _lock.lock();

            if (_isShutdown) {
                _lock.unlock();
                throw new IllegalStateException("already shutdown");
            }

            V obj = _puts.poll();
            while (_destroyedObjects.contains(obj)) {
                // expired object, clean it up and try again
                _destroyedObjects.remove(obj);
                objects.decrementAndGet();

                _lock.unlock();
                destroy(obj);
                _lock.lock();

                obj = _puts.poll();
            }

            if (obj != null) {

                // we got one, send it out
                _lock.unlock();
                c.handleObject(obj);
                return true;
            } else {

                // we didn't get one, try to enqueue our request
                // or reject the request if there are too many already
                boolean success = (skipToFront ? _takes.offerFirst(c) : _takes.offerLast(c));
                _lock.unlock();
                if (!success) {
                    rejected.incrementAndGet();
                    throw new RejectedExecutionException();
                }
                return false;
            }
        }
    }

    // static field
    private static AtomicInteger _numPools = new AtomicInteger(0);

    // fields
    private final int _maxQueueSize;
    private final Generator<K,V> _generator;
    private final Controller<K> _controller;

    private Map<K,Stats> _stats;
    private boolean _isShutdown = false;

    private final AtomicInteger _numObjects = new AtomicInteger(0);
    private final ReentrantLock _lock = new ReentrantLock();
    private final Set<V> _destroyedObjects = Collections.synchronizedSet(new HashSet<V>());
    private final ConcurrentHashMap<V,Long> _start = new ConcurrentHashMap<V,Long>();
    private final ConcurrentHashMap<K,Queue> _queues = new ConcurrentHashMap<K,Queue>();

    private final Stats.UniformLongReservoirMap<K> _queueLatencies = new Stats.UniformLongReservoirMap<K>();
    private final Stats.UniformLongReservoirMap<K> _taskLatencies = new Stats.UniformLongReservoirMap<K>();
    private final Stats.UniformLongReservoirMap<K> _queueLengths = new Stats.UniformLongReservoirMap<K>();
    private final Stats.UniformDoubleReservoirMap<K> _utilizations = new Stats.UniformDoubleReservoirMap<K>();
    private final Stats.UniformDoubleReservoirMap<K> _taskArrivalRates = new Stats.UniformDoubleReservoirMap<K>();
    private final Stats.UniformDoubleReservoirMap<K> _taskCompletionRates = new Stats.UniformDoubleReservoirMap<K>();
    private final Stats.UniformDoubleReservoirMap<K> _taskRejectionRates = new Stats.UniformDoubleReservoirMap<K>();

    // private methods

    /**
     * Returns or creates the queue for the given key.
     */
    private Queue queue(K key) {
        Queue q = _queues.get(key);
        if (q == null) {
            q = new Queue(key, _maxQueueSize);
            Queue prior = (Queue) _queues.putIfAbsent(key, q);
            return prior == null ? q : prior;
        }
        return q;
    }

    private Map<K,Stats> updateStats() {
        Map<K,long[]> queueLatencies = _queueLatencies.toMap();
        Map<K,long[]> taskLatencies = _taskLatencies.toMap();
        Map<K,long[]> queueLengths = _queueLengths.toMap();
        Map<K,double[]> utilizations = _utilizations.toMap();
        Map<K,double[]> taskArrivalRates = _taskArrivalRates.toMap();
        Map<K,double[]> taskCompletionRates = _taskCompletionRates.toMap();
        Map<K,double[]> taskRejectionRates = _taskRejectionRates.toMap();

        Map<K,Stats> stats = new HashMap<K,Stats>();
        for (K key : _queues.keySet()) {
            stats.put(key,
                      new Stats(EnumSet.allOf(Stats.Metric.class),
                                queue(key).objects.get(),
                                utilizations.get(key),
                                taskArrivalRates.get(key),
                                taskCompletionRates.get(key),
                                taskRejectionRates.get(key),
                                queueLengths.get(key),
                                queueLatencies.get(key),
                                taskLatencies.get(key)));
        }
        return stats;
    }

    private void addObject(K key) {
        Queue q = queue(key);

        _lock.lock();
        if (_controller.shouldIncrement(key, q.objects.get(), _numObjects.get())) {

            // get all of our numbers aligned before unlocking
            _numObjects.incrementAndGet();
            q.objects.incrementAndGet();
            _lock.unlock();

            try {
                q.put(_generator.generate(key));
            } catch (Exception e) {
                _numObjects.decrementAndGet();
                q.objects.decrementAndGet();
                throw new RuntimeException(e);
            }
        } else {
            _lock.unlock();
        }
    }

    private void startControlLoop(int duration, int iterations) {

        double samplesPerSecond = 1000.0 / duration;
        int iteration = 0;

        try {
            while (!_isShutdown) {

                iteration = (iteration + 1) % iterations;

                long start = System.currentTimeMillis();

                for (Map.Entry<K, Queue> entry : _queues.entrySet()) {
                    K key = entry.getKey();
                    Queue q = entry.getValue();
                    long completed = q.completed.getAndSet(0);
                    long incoming = q.incoming.getAndSet(0);
                    long rejected = q.rejected.getAndSet(0);
                    int objects = q.objects.get();

                    _queueLengths.sample(key, q.getQueueLength());
                    _taskArrivalRates.sample(key, incoming);
                    _taskCompletionRates.sample(key, completed);
                    _taskRejectionRates.sample(key, rejected);

                    int queueLength = q.getQueueLength();
                    double utilization = (double) (queueLength > 0 ? (objects + queueLength) : (incoming - completed)) / Math.max(1, objects);
                    _utilizations.sample(key, utilization);
                }

                if (_isShutdown) {
                    break;
                }

                // update worker count
                if (iteration == 0) {
                    _stats = updateStats();
                    Map<K,Integer> adjustment = _controller.adjustment(_stats);

                    // clear out any unused queues
                    _lock.lock();
                    for (Map.Entry<K,Stats> entry : _stats.entrySet()) {
                        K key = entry.getKey();
                        if (entry.getValue().getUtilization(1) == 0
                            && _queues.get(key).objects.get() == 0) {
                            _queues.remove(key).shutdown();
                        }
                    }
                    _lock.unlock();

                    // defer pool growth until we've reduced other pools
                    List<K> upward = new ArrayList<K>();

                    for (Map.Entry<K,Integer> entry : adjustment.entrySet()) {
                        int n = entry.getValue().intValue();
                        if (n < 0) {
                            Queue q = queue(entry.getKey());
                            for (int i = 0; i < -n; i++) {
                                q.drop();
                            }
                            q.cleanup();
                        } else if (n > 1) {
                            for (int i = 0; i < n; i++) {
                                upward.add(entry.getKey());
                            }
                        }
                    }

                    // if we don't have room for everything, make sure we grow
                    // a random subset
                    Collections.shuffle(upward);
                    for (K key : upward) {
                        Queue q = queue(key);
                        addObject(key);
                    }
                }

                Thread.sleep(Math.max(0, duration - (System.currentTimeMillis() - start)));
            }
        } catch (InterruptedException e) {

        }
    }

    // constructor

    public Pool(Generator<K,V> generator, Controller<K> controller, int maxQueueSize, long samplePeriod, long controlPeriod, TimeUnit unit) {
        _generator = generator;
        _controller = controller;
        _maxQueueSize = maxQueueSize;

        final int duration = (int) unit.toMillis(samplePeriod);
        final int iterations = (int) (controlPeriod / samplePeriod);

        Thread t =
            new Thread(new Runnable() {
                    public void run() {
                        startControlLoop(duration, iterations);
                    }
                },
                "dirigiste-pool-controller-" + _numPools.getAndIncrement());
        t.setDaemon(true);
        t.start();

    }

    // public methods

    /**
     * Acquires an object from the pool, potentially creating one if none is available.
     *
     * @param key  the key of the pooled object being acquired
     * @param callback  the callback that will be invoked with the object once it's available
     */
    public void acquire(final K key, final AcquireCallback<V> callback) {
        final long start = System.nanoTime();

        Queue q = queue(key);
        boolean success =
            q.take(new AcquireCallback<V>() {
                    public void handleObject(V obj) {

                        // do all the latency bookkeeping
                        long acquire = System.nanoTime();
                        _queueLatencies.sample(key, acquire - start);
                        _start.put(obj, new Long(start));

                        callback.handleObject(obj);
                    }
                }, false);

        // if we didn't immediately get an object, try to create one
        if (!success) {
            addObject(key);
        }
    }

    /**
     * Acquires an object from the pool, potentially creating one if none is available.
     *
     * @param key  the key of the pooled object being acquired
     * @return the object, once it's acquired
     */
    public V acquire(K key) throws InterruptedException {
        final AtomicReference<V> ref = new AtomicReference<V>(null);
        final CountDownLatch latch = new CountDownLatch(1);

        acquire(key, new AcquireCallback<V>() {
                public void handleObject(V obj) {
                    ref.set(obj);
                    latch.countDown();
                }
            });

        latch.await();
        return ref.get();
    }

    /**
     * Releases an object that has been acquired back to the pool.
     *
     * @param key  the key of the pooled object being released
     * @param obj  the pooled object being released
     */
    public void release(K key, V obj) {
        long end = System.nanoTime();
        Long start = _start.remove(obj);

        if (start == null) {
            throw new IllegalStateException("tried to release an object that hasn't been acquired");
        }

        _taskLatencies.sample(key, end - start.longValue());
        queue(key).put(obj);
    }

    /**
     * Disposes of an object, removing it from the pool.
     *
     * @param key  the key of the pooled object being disposed
     * @param obj  the pooled object being disposed
     */
    public void dispose(K key, V obj) {
        Queue q = queue(key);

        _lock.lock();
        _destroyedObjects.add(obj);
        Long start = _start.remove(obj);
        _lock.unlock();

        // if it's been taken, "put" it back so it can be cleaned up
        if (start != null) {
            q.put(obj);
        } else {
            q.cleanup();
        }
    }

    public void shutdown() {
        _isShutdown = true;
        for (Map.Entry<K,Queue> entry : _queues.entrySet()) {
            entry.getValue().shutdown();
        }
    }
}
