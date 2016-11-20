package io.aleph.dirigiste;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

public class Pool<K,V> implements IPool<K,V> {

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

        public void cancelTake(AcquireCallback<V> take) {
            _takes.remove(take);
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

                // if we're already at zero, or have more work to do
                // it's a no-op
                if (n <= 0 || getQueueLength() > 0) {
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
                _destroyedObjects.remove(obj);
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
    private final Set<V> _destroyedObjects = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<V, Boolean>()));
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

    @Override
    public void acquire(final K key, final AcquireCallback<V> callback) {
        final long start = System.nanoTime();

        Queue q = queue(key);
        AcquireCallback<V> wrapper =
            new AcquireCallback<V>() {
                    public void handleObject(V obj) {

                        // do all the latency bookkeeping
                        long acquire = System.nanoTime();
                        _queueLatencies.sample(key, acquire - start);
                        _start.put(obj, new Long(start));

                        callback.handleObject(obj);
                    }
            };
        boolean success = q.take(wrapper, false);

        // if we didn't immediately get an object, try to create one
        if (!success) {
            try {
                addObject(key);
            } catch (Throwable e) {
                q.cancelTake(wrapper);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
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

    @Override
    public void release(K key, V obj) {
        long end = System.nanoTime();
        Long start = _start.remove(obj);

        if (start != null) {
            _taskLatencies.sample(key, end - start.longValue());
            queue(key).put(obj);
        }
    }

    @Override
    public void dispose(K key, V obj) {
        Queue q = queue(key);

        _lock.lock();
        _destroyedObjects.add(obj);
        int pendingTakes = q._takes.size();
        Long start = _start.remove(obj);
        _lock.unlock();

        // if it's been taken, "put" it back so it can be cleaned up
        if (start != null) {
            q.put(obj);
        } else {
            q.cleanup();
        }

        if (pendingTakes > 0) {
            addObject(key);
        }
    }

    @Override
    public void shutdown() {
        _isShutdown = true;
        for (Map.Entry<K,Queue> entry : _queues.entrySet()) {
            entry.getValue().shutdown();
        }
    }
}
