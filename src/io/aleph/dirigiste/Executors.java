package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.EnumSet;

public class Executors {

    private static ThreadFactory threadFactory() {
        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = java.util.concurrent.Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            }
        };
    }

    /**
     * @param numThreads  the number of threads in the thread pool
     */
    public static Executor fixedExecutor(final int numThreads) {
        return fixedExecutor(numThreads, EnumSet.noneOf(Stats.Metric.class));
    }

    /**
     * @param numThreads  the number of threads in the thread pool
     * @param metrics  the metrics that will be gathered by the executor
     */
    public static Executor fixedExecutor(final int numThreads, EnumSet<Stats.Metric> metrics) {
        return new Executor(threadFactory(), new SynchronousQueue(false), fixedController(numThreads), numThreads, metrics, 25, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * @param numThreads  the number of threads in the thread pool
     */
    public static Executor.Controller fixedController(final int numThreads) {
        return new Executor.Controller() {
            public boolean shouldIncrement(int numWorkers) {
                return numWorkers < numThreads;
            }

            public int adjustment(Stats stats) {
                return stats.getNumWorkers() - numThreads;
            }
        };
    }

    /**
     * @param targetUtilization  the target level of utilization, within [0, 1]
     * @param maxThreadCount  the maximum number of threads
     */
    public static Executor utilizationExecutor(double targetUtilization, int maxThreadCount) {
        return utilizationExecutor(targetUtilization, maxThreadCount, EnumSet.of(Stats.Metric.UTILIZATION));
    }

    /**
     * @param targetUtilization  the target level of utilization, within [0, 1]
     * @param maxThreadCount  the maximum number of threads
     * @param metrics  the metrics which should be gathered
     */
    public static Executor utilizationExecutor(double targetUtilization, int maxThreadCount, EnumSet<Stats.Metric> metrics) {
        return new Executor(threadFactory(), new SynchronousQueue(false), utilizationController(targetUtilization, maxThreadCount), 1, metrics, 25, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * @param targetUtilization  the target level of utilization, within [0, 1]
     * @param maxThreadCount  the maximum number of threads that can be allocated
     */
     public static Executor.Controller utilizationController(final double targetUtilization, final int maxThreadCount) {
        return new Executor.Controller() {
            public boolean shouldIncrement(int numWorkers) {
                return numWorkers < maxThreadCount;
            }

            public int adjustment(Stats stats) {
                int numWorkers = stats.getNumWorkers();
                double correction = stats.getUtilization(1.0) / targetUtilization;
                int n = (int) Math.ceil(stats.getNumWorkers() * correction) - numWorkers;

                if (n < 0) {
                    return Math.max(n, (int) -Math.ceil(numWorkers/4.0));
                } else if (n > 0) {
                    return Math.min(n, (int) Math.ceil(numWorkers/4.0));
                } else {
                    return 0;
                }
            }
        };
    }

}
