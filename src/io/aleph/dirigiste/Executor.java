package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.List;
import java.util.ArrayList;
import java.util.EnumSet;

public class Executor extends AbstractExecutorService {

    public interface Controller {

        /**
         * @param currThreads the current number of active threads
         * @return whether an additional thread should be spun up, a return value of false may cause a RejectedExecutionException to be thrown elsewhere
         */
        boolean shouldIncrement(int currThreads);

        /**
         * @param stats the statistics gathered since the last call to 'adjustment'
         * @return if positive, the number of threads that should be spun up, if negative the number of threads that should be spun down
         */
        int adjustment(Stats stats);
    }

    class Worker {
        public volatile Runnable _runnable;
        public volatile boolean _isShutdown = false;

        private final AtomicInteger _completed = new AtomicInteger(0);

        private long _birth = System.nanoTime();
        private final AtomicLong _start = new AtomicLong(0);
        private final AtomicLong _totalDuration = new AtomicLong(0);

        private final CountDownLatch _latch = new CountDownLatch(1);
        private final Thread _thread;

        Worker() {

            final boolean taskCompletionRate = _metrics.contains(Stats.Metric.TASK_COMPLETION_RATE);
            final boolean workerUtilization = _metrics.contains(Stats.Metric.UTILIZATION);

            Runnable runnable =
                new Runnable() {
                    public void run() {
                        try {

                            _birth = System.nanoTime();

                            while (!_isShutdown) {
                                Runnable r = (Runnable) _queue.poll(1000, TimeUnit.MILLISECONDS);

                                if (r != null) {
                                    _runnable = r;

                                    if (workerUtilization) {
                                        _start.set(System.nanoTime());
                                    }

                                    try {
                                        r.run();
                                    } catch (Throwable e) {

                                    } finally {
                                        _runnable = null;

                                        if (workerUtilization) {
                                            _totalDuration.addAndGet(System.nanoTime() - _start.getAndSet(0));
                                        }

                                        if (taskCompletionRate) {
                                            _completed.incrementAndGet();
                                        }
                                    }
                                }
                            }
                        } catch (InterruptedException e) {

                        }
                        _workers.remove(Worker.this);
                        _latch.countDown();
                    }
                };

            _thread = _threadFactory.newThread(runnable);
            _thread.start();
        }

        public double utilization(long t0, long t1) {
            long start = _start.getAndSet(t1);
            if (start == 0) {
                _start.compareAndSet(t1, 0);
            }
            long active = _totalDuration.getAndSet(0) + (start == 0 ? 0 : t1 - start);
            long total = t1 - Math.max(t0, _birth);

            return (double) active / (double) total;
        }

        public boolean isActive() {
            return _runnable != null;
        }

        public boolean isShutdown() {
            return _isShutdown;
        }

        public boolean shutdown() {
            if (!_isShutdown) {
                _isShutdown = true;
                _numWorkers.decrementAndGet();
                return true;
            }
            return false;
        }
    }

    private static AtomicInteger _numExecutors = new AtomicInteger(0);

    private final ThreadFactory _threadFactory;
    private final BlockingQueue _queue;
    private final CopyOnWriteArrayList<Worker> _workers = new CopyOnWriteArrayList<Worker>();
    private final AtomicInteger _numWorkers = new AtomicInteger(0);
    private final AtomicInteger _incomingTasks = new AtomicInteger(0);
    private final AtomicInteger _rejectedTasks = new AtomicInteger(0);
    private final Controller _controller;

    private final EnumSet _metrics;
    private final boolean _measureQueueLatency;
    private final boolean _measureTaskLatency;
    private final boolean _measureTaskArrivalRate;
    private final boolean _measureTaskRejectionRate;

    private boolean _isShutdown = false;

    private final AtomicReference<Stats.UniformLongReservoir> _queueLatencies =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformLongReservoir> _taskLatencies =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformLongReservoir> _queueLengths =
        new AtomicReference<Stats.UniformLongReservoir>(new Stats.UniformLongReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _utilizations =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _taskArrivalRates =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _taskCompletionRates =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private final AtomicReference<Stats.UniformDoubleReservoir> _taskRejectionRates =
        new AtomicReference<Stats.UniformDoubleReservoir>(new Stats.UniformDoubleReservoir());

    private volatile Stats _stats = Stats.EMPTY;

    /**
     * @param threadFactory the ThreadFactory used by the executor
     * @param queue  the queue that holds Runnable objects waiting to be executed
     * @param controller  the Controller object that updates the thread count
     * @param metrics  the metrics that will be collected and delivered to the controller
     * @param initialThreadCount  the number of threads that the executor will begin with
     * @param samplePeriod  the period at which the executor's state will be sampled
     * @param controlPeriod  the period at which the controller will be invoked with the gathered statistics
     * @param unit  the time unit for the #samplePeriod and #controlPeriod
     */
    public Executor(ThreadFactory threadFactory, BlockingQueue queue, Executor.Controller controller, int initialThreadCount, EnumSet<Stats.Metric> metrics, long samplePeriod, long controlPeriod, TimeUnit unit) {

        _threadFactory = threadFactory;
        _queue = queue;
        _controller = controller;
        _metrics = metrics;

        _measureQueueLatency = _metrics.contains(Stats.Metric.QUEUE_LATENCY);
        _measureTaskLatency = _metrics.contains(Stats.Metric.TASK_LATENCY);
        _measureTaskArrivalRate = _metrics.contains(Stats.Metric.TASK_ARRIVAL_RATE);
        _measureTaskRejectionRate = _metrics.contains(Stats.Metric.TASK_REJECTION_RATE);

        final int duration = (int) unit.toMillis(samplePeriod);
        final int iterations = (int) (controlPeriod / samplePeriod);

        Thread t =
            new Thread(new Runnable() {
                    public void run() {
                        startControlLoop(duration, iterations);
                    }
                },
                "dirigiste-executor-controller-" + _numExecutors.getAndIncrement());
        t.setDaemon(true);
        t.start();

        for (int i = 0; i < Math.max(1, initialThreadCount); i++) {
            startWorker();
        }
    }

    /**
     * @return the metrics being gathered by the executor
     */
    public EnumSet<Stats.Metric> getMetrics() {
        return _metrics;
    }

    /**
     * @return the last aggregate statistics given to the control loop.
     */
    public Stats getLastStats() {
        return _stats;
    }

    /**
     * @return the aggregate statistics for the executor since the last control loop update.
     */
    public Stats getStats() {
        return new Stats
            (_metrics,
             _numWorkers.get(),
             _utilizations.get().toArray(),
             _taskArrivalRates.get().toArray(),
             _taskCompletionRates.get().toArray(),
             _taskRejectionRates.get().toArray(),
             _queueLengths.get().toArray(),
             _queueLatencies.get().toArray(),
             _taskLatencies.get().toArray());
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

    /**
     * A version of execute which will simply block until the task is accepted, rather than
     * throwing a RejectedExceptionException.
     *
     * RejectedExecutionException will only be thrown if the executor is shut down.
     */
    public void executeWithoutRejection(Runnable runnable) throws NullPointerException, InterruptedException {
        if (runnable == null) {
            throw new NullPointerException();
        }

        if (_isShutdown) {
            throw new RejectedExecutionException("Executor is shutdown!");
        }

        if (_measureTaskArrivalRate) {
            _incomingTasks.incrementAndGet();
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
            startWorker();
            _queue.put(runnable);
        }
    }

    @Override
    public void execute(Runnable runnable) throws NullPointerException, RejectedExecutionException {
        if (runnable == null) {
            throw new NullPointerException();
        }

        if (_isShutdown) {
            throw new RejectedExecutionException("Executor is shutdown!");
        }

        if (_measureTaskArrivalRate) {
            _incomingTasks.incrementAndGet();
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
            if (startWorker()) {
                try {
                    _queue.put(runnable);
                } catch (InterruptedException e) {
                    if (_measureTaskRejectionRate) {
                        _rejectedTasks.incrementAndGet();
                    }
                    throw new RejectedExecutionException();
                }
            } else {
                if (_measureTaskRejectionRate) {
                    _rejectedTasks.incrementAndGet();
                }
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
        synchronized (this) {
            _isShutdown = true;
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

    ///

    private Stats updateStats() {
        return new Stats
            (_metrics,
             _numWorkers.get(),
             _utilizations.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _taskArrivalRates.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _taskCompletionRates.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _taskRejectionRates.getAndSet(new Stats.UniformDoubleReservoir()).toArray(),
             _queueLengths.getAndSet(new Stats.UniformLongReservoir()).toArray(),
             _queueLatencies.getAndSet(new Stats.UniformLongReservoir()).toArray(),
             _taskLatencies.getAndSet(new Stats.UniformLongReservoir()).toArray());
    }

    private boolean startWorker() {
        while (true) {
            int numWorkers = _numWorkers.get();
            if (!_controller.shouldIncrement(numWorkers)) {
                return false;
            }
            if (_numWorkers.compareAndSet(numWorkers, numWorkers+1)) {
                _workers.add(new Worker());
                return true;
            }
        }
    }

    private void startControlLoop(int duration, int iterations) {

        boolean measureUtilization = _metrics.contains(Stats.Metric.UTILIZATION);
        boolean measureTaskArrivalRate = _metrics.contains(Stats.Metric.TASK_ARRIVAL_RATE);
        boolean measureTaskCompletionRate = _metrics.contains(Stats.Metric.TASK_COMPLETION_RATE);
        boolean measureTaskRejectionRate = _metrics.contains(Stats.Metric.TASK_REJECTION_RATE);
        boolean measureQueueLength = _metrics.contains(Stats.Metric.QUEUE_LENGTH);

        double samplesPerSecond = 1000.0 / duration;
        int iteration = 0;
        long utilizationSample = 0;

        try {
            while (!_isShutdown) {
                iteration = (iteration + 1) % iterations;

                long start = System.currentTimeMillis();

                // gather stats
                if (measureQueueLength) {
                    _queueLengths.get().sample(_queue.size());
                }

                if (measureTaskArrivalRate) {
                    _taskArrivalRates.get().sample(_incomingTasks.getAndSet(0) * samplesPerSecond);
                }

                if (measureTaskRejectionRate) {
                    _taskRejectionRates.get().sample(_rejectedTasks.getAndSet(0) * samplesPerSecond);
                }

                int tasks = 0;

                int workerCount = 0;
                double utilizationSum = 0.0;
                long nextUtilizationSample = 0;
                if (measureUtilization) {
                    nextUtilizationSample = System.nanoTime();
                }

                for (Worker w : _workers) {
                    if (w.isShutdown()) {
                        continue;
                    }

                    if (measureUtilization) {
                        workerCount++;
                        utilizationSum += w.utilization(utilizationSample, nextUtilizationSample);
                    }

                    if (measureTaskCompletionRate) {
                        tasks += w._completed.getAndSet(0);
                    }
                }

                if (measureUtilization) {
                    utilizationSample = nextUtilizationSample;
                    _utilizations.get().sample(utilizationSum / (double) workerCount);
                }

                if (measureTaskCompletionRate) {
                    _taskCompletionRates.get().sample(tasks * samplesPerSecond);
                }

                // update worker count
                if (iteration == 0) {
                    _stats = updateStats();
                    int adjustment = _controller.adjustment(_stats);

                    synchronized (this) {
                        if (_isShutdown) {
                            break;
                        }

                        if (adjustment < 0 && _queue.size() == 0) {

                            // never let the number of workers drop below 1
                            adjustment = Math.min(-adjustment, _numWorkers.get()-1);

                            for (Worker w : _workers) {
                                if (adjustment == 0) break;
                                if (w.shutdown()) {
                                    adjustment--;
                                }
                            }
                        } else if (adjustment > 0) {

                            // create new workers
                            for (int i = 0; i < adjustment; i++) {
                                if (!startWorker()) {
                                    break;
                                }
                            }
                        }
                    }
                }

                Thread.sleep(Math.max(0, duration - (System.currentTimeMillis() - start)));
            }
        } catch (InterruptedException e) {

        }
    }
}
