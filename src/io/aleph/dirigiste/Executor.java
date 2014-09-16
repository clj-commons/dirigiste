package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.List;
import java.util.ArrayList;
import java.util.EnumSet;

public class Executor extends AbstractExecutorService {

    public enum Metrics {
        QUEUE_LENGTH,
        QUEUE_LATENCY,
        TASK_LATENCY,
        TASK_RATE,
        UTILIZATION
    }



    private final ThreadFactory _threadFactory;
    private final BlockingQueue _queue;
    private final CopyOnWriteArrayList<Worker> _workers = new CopyOnWriteArrayList<Worker>();
    private final AtomicInteger _workerCount = new AtomicInteger(0);
    private final Controller _controller;

    private final EnumSet _metrics;
    private final boolean _measureQueueLatency;
    private final boolean _measureTaskLatency;

    private boolean _isShutdown = false;

    private final AtomicReference<Stats.UniformLongReservoir> _queueLatencies =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformLongReservoir> _taskLatencies =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformLongReservoir> _queueLengths =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _utilizations =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _taskRates =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private volatile Stats _stats = Stats.EMPTY;

    class Worker {
        public volatile Runnable _runnable;
        public volatile boolean _isShutdown = false;

        private final AtomicInteger _completed = new AtomicInteger(0);
        private final CountDownLatch _latch = new CountDownLatch(1);
        private final Thread _thread;

        Worker() {

            final boolean taskRate = _metrics.contains(Metrics.TASK_RATE);

            Runnable runnable =
                new Runnable() {
                    public void run() {
                        try {
                            while (!_isShutdown) {
                                Runnable r = (Runnable) _queue.poll(1000, TimeUnit.MILLISECONDS);

                                if (r != null) {
                                    _runnable = r;

                                    try {
                                        r.run();
                                    } catch (Throwable e) {

                                    } finally {
                                        _runnable = null;
                                        if (taskRate) {
                                            _completed.incrementAndGet();
                                        }
                                    }
                                }
                            }
                        } catch (InterruptedException e) {

                        }
                        _workers.remove(this);
                        _latch.countDown();
                    }
                };

            _thread = _threadFactory.newThread(runnable);
            _thread.start();
            _workerCount.incrementAndGet();
        }

        public boolean isActive() {
            return _runnable != null;
        }

        public boolean shutdown() {
            if (!_isShutdown) {
                _isShutdown = true;
                _workerCount.decrementAndGet();
                return true;
            }
            return false;
        }
    }

    private static AtomicInteger _numExecutors = new AtomicInteger(0);

    /**
     * @param threadFactory the ThreadFactory used by the executor
     * @param queue the queue that holds Runnable objects waiting to be executed
     * @param controller the Controller object that
     */
    public Executor(ThreadFactory threadFactory, BlockingQueue queue, Controller controller, EnumSet<Metrics> metrics, long samplePeriod, long controlPeriod, TimeUnit unit) {

        _threadFactory = threadFactory;
        _queue = queue;
        _controller = controller;
        _metrics = metrics;
        _measureQueueLatency = _metrics.contains(Metrics.QUEUE_LATENCY);
        _measureTaskLatency = _metrics.contains(Metrics.TASK_LATENCY);

        final int duration = (int) unit.toMillis(samplePeriod);
        final int iterations = (int) (controlPeriod / samplePeriod);

        new Thread(new Runnable() {
                public void run() {
                    startControlLoop(duration, iterations);
                }
            },
            "dirigiste-controller-" + _numExecutors.getAndIncrement()).start();
    }

    private void startControlLoop(int duration, int iterations) {

        boolean measureUtilization = _metrics.contains(Metrics.UTILIZATION);
        boolean measureTaskRate = _metrics.contains(Metrics.TASK_RATE);
        boolean measureQueueLength = _metrics.contains(Metrics.QUEUE_LENGTH);

        double samplesPerSecond = 1000.0 / duration;
        int iteration = 0;

        try {
            while (!_isShutdown) {
                iteration = (iteration + 1) % iterations;

                long start = System.currentTimeMillis();

                // gather stats
                if (measureQueueLength) {
                    _queueLengths.get().sample(_queue.size());
                }

                int active = 0;
                int cnt = _workers.size();
                int tasks = 0;
                for (Worker w : _workers) {
                    if (w.isActive()) {
                        active++;
                    }
                    tasks += w._completed.getAndSet(0);
                }

                if (measureUtilization) {
                    _utilizations.get().sample((double) active / cnt);
                }

                if (measureTaskRate) {
                    _taskRates.get().sample(tasks * samplesPerSecond);
                }

                // update worker count
                if (iteration == 0) {
                    _stats = updateStats();
                    int adjustment = _controller.adjustment(_stats);

                    synchronized (this) {
                        if (_isShutdown) {
                            break;
                        }

                        if (adjustment < 0) {

                            adjustment = -adjustment;
                            for (Worker w : _workers) {
                                if (adjustment == 0) break;
                                if (w.shutdown()) {
                                    adjustment--;
                                }
                            }
                        } else if (adjustment > 0) {

                            // create new workers
                            for (int i = 0; i < adjustment; i++) {
                                if (!_controller.shouldIncrement(_workerCount.get())) {
                                    break;
                                }
                                _workers.add(new Worker());
                            }
                        }
                    }
                }

                Thread.sleep(Math.max(0, duration - (System.currentTimeMillis() - start)));
            }
        } catch (InterruptedException e) {

        }
    }

    /**
     * @returns the last aggregate statistics given to the control loop.
     */
    public Stats getLastStats() {
        return _stats;
    }

    /**
     * @returns the aggregate statistics for the executor since the last control loop update.
     */
    public Stats getStats() {
        return new Stats
            (_workerCount.get(),
             _utilizations.get().toArray(),
             _taskRates.get().toArray(),
             _queueLengths.get().toArray(),
             _queueLatencies.get().toArray(),
             _taskLatencies.get().toArray());
    }

    private Stats updateStats() {
        return new Stats
            (_workerCount.get(),
             _utilizations.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _taskRates.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _queueLengths.getAndSet(new Stats.UniformLongReservoir()).toArray(),
             _queueLatencies.getAndSet(new Stats.UniformLongReservoir()).toArray(),
             _taskLatencies.getAndSet(new Stats.UniformLongReservoir()).toArray());
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {

        long duration = unit.toMillis(timeout);
        long start = System.currentTimeMillis();

        for (Worker w : _workers) {
            long remaining = (start + duration) - System.currentTimeMillis();
            if (remaining < 0) {
                return false;
            }
            w._latch.await(remaining, TimeUnit.MILLISECONDS);
        }

        return true;
    }

    @Override
    public void execute(Runnable runnable) throws NullPointerException, RejectedExecutionException {
        if (runnable == null) {
            throw new NullPointerException();
        }

        if (_measureTaskLatency || _measureQueueLatency) {
            final long enqueue = System.nanoTime();
            final Runnable r = runnable;
            runnable = new Runnable() {
                    public void run() {

                        if (_measureQueueLatency) {
                            _queueLatencies.get().sample(System.nanoTime() - enqueue);
                        }

                        try {
                            r.run();
                        } finally {
                            if (_measureTaskLatency) {
                                _taskLatencies.get().sample(System.nanoTime() - enqueue);
                            }
                        }
                    }
                };
        }

        if (!_queue.offer(runnable) || _workers.isEmpty()) {
            if (_controller.shouldIncrement(_workerCount.get())) {
                _workers.add(new Worker());
                try {
                    _queue.put(runnable);
                } catch (InterruptedException e) {
                    throw new RejectedExecutionException();
                }
            } else {
                throw new RejectedExecutionException();
            }
        }
    }

    @Override
    public boolean isShutdown() {
        return _isShutdown;
    }

    @Override
    public boolean isTerminated() {
        return _isShutdown && _workers.isEmpty();
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            _isShutdown = true;
            for (Worker w : _workers) {
                w.shutdown();
            }
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> rs = new ArrayList<Runnable>();
        _queue.drainTo(rs);
        for (Worker w : _workers) {
            Runnable r = w._runnable;
            w.shutdown();
            w._thread.interrupt();

            if (r != null) {
                rs.add(r);
            }
        }
        return rs;
    }
}
