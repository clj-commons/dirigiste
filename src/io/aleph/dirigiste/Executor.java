package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.List;
import java.util.ArrayList;

public class Executor extends AbstractExecutorService {

    public interface Controller {
        boolean shouldIncrement(int currThreads);
        int adjustment(Stats stats);
    }

    private final ThreadFactory _threadFactory;
    private final BlockingQueue _queue;
    private final CopyOnWriteArrayList<Worker> _workers = new CopyOnWriteArrayList<Worker>();
    private final AtomicInteger _workerCount = new AtomicInteger(0);
    private final Controller _controller;

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
                                        _completed.incrementAndGet();
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

    public Executor(ThreadFactory threadFactory, BlockingQueue queue, Controller controller, long samplePeriod, long controlPeriod, TimeUnit unit) {
        _threadFactory = threadFactory;
        _queue = queue;
        _controller = controller;

        final int duration = (int) unit.toMillis(samplePeriod);
        final int iterations = (int) (controlPeriod / samplePeriod);

        new Thread(new Runnable() {
                public void run() {
                    startControlLoop(duration, iterations);
                }
            },
            "dirigiste-controller-" + _numExecutors.getAndIncrement()).start();
    }

    /**
     * Returns an executor which aims for a level of utilization, from 0 to 1, but will not
     * use more than maxThreads at a time.
     */
    public static Executor utilizationExecutor(final double targetUtilization, final int maxThreadCount) {
        Controller controller = new Controller() {
                public boolean shouldIncrement(int numWorkers) {
                    return numWorkers < maxThreadCount;
                }

                public int adjustment(Stats stats) {
                    double utilization = stats.getUtilization(0.9) / targetUtilization;
                    return (int) (stats.getWorkerCount() * utilization) - stats.getWorkerCount();
                }
            };

        return new Executor(Executors.defaultThreadFactory(), new SynchronousQueue(), controller, 25, 10000, TimeUnit.MILLISECONDS);
    }

    private void startControlLoop(int duration, int iterations) {

        final double samplesPerSecond = 1000.0 / duration;
        int iteration = 0;

        try {
            while (!_isShutdown) {
                iteration = (iteration + 1) % iterations;

                long start = System.currentTimeMillis();

                // gather stats
                _queueLengths.get().sample(_queue.size());
                int active = 0;
                int cnt = _workers.size();
                int tasks = 0;
                for (Worker w : _workers) {
                    if (w.isActive()) {
                        active++;
                    }
                    tasks += w._completed.getAndSet(0);
                }
                _utilizations.get().sample((double) active / cnt);
                _taskRates.get().sample(tasks * samplesPerSecond);

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
     * Returns the last aggregate statistics given to the control loop.
     */
    public Stats getLastStats() {
        return _stats;
    }

    /**
     * Calculates and returns the aggregate statistics for the executor since the last
     * control loop update.
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

        final long enqueue = System.nanoTime();
        final Runnable r = runnable;
        runnable = new Runnable() {
                public void run() {
                    long start = System.nanoTime();
                    _queueLatencies.get().sample(start - enqueue);
                    try {
                        r.run();
                    } finally {
                        long end = System.nanoTime();
                        _taskLatencies.get().sample(end - enqueue);
                    }
                }
            };

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
