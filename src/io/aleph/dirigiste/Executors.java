package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.EnumSet;

public class Executors {

    private static ThreadFactory defaultThreadFactory = java.util.concurrent.Executors.defaultThreadFactory();

    /**
     * @param targetUtilization  the target level of utilization, from 0 to 1
     * @param maxThreadCount  the maximum number of threads
     */
    public static Executor utilization(double targetUtilization, int maxThreadCount) {
        return utilization(targetUtilization, maxThreadCount, EnumSet.of(Executor.Metric.UTILIZATION));
    }

    /**
     * @param targetUtilization  the target level of utilization, from 0 to 1
     * @param maxThreadCount  the maximum number of threads
     * @param metrics  the metrics which should be gathered
     */
    public static Executor utilization(double targetUtilization, int maxThreadCount, EnumSet<Executor.Metric> metrics) {
        return new Executor(defaultThreadFactory, new SynchronousQueue(), Controllers.utilization(targetUtilization, maxThreadCount), metrics, 25, 10000, TimeUnit.MILLISECONDS);
    }


}
