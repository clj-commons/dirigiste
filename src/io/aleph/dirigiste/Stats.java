package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.Random;
import java.util.Arrays;

public class Stats {

    private static ThreadLocal<Random> _randoms = new ThreadLocal<Random>() {
        protected Random initialValue() {
            return new Random();
        }
    };

    static class UniformLongReservoir {

        private final AtomicInteger _count = new AtomicInteger();
        private final AtomicLongArray _values = new AtomicLongArray(1024);

        UniformLongReservoir() {
        }

        public void sample(long n) {
            int cnt = _count.incrementAndGet();
            if (cnt <= 1024) {
                _values.set(cnt-1, n);
            } else {
                int idx = _randoms.get().nextInt(cnt);
                if (idx < 1024) {
                    _values.set(idx, n);
                }
            }
        }

        public long[] toArray() {
            int cnt = Math.min(1024, _count.get());

            long[] vals = new long[cnt];
            for (int i = 0; i < cnt; i++) {
                vals[i] = _values.get(i);
            }
            Arrays.sort(vals);

            return vals;
        }
    }

    static class UniformDoubleReservoir {
        private static ThreadLocal<Random> _randoms = new ThreadLocal<Random>() {
            protected Random initialValue() {
                return new Random();
            }
        };

        private final AtomicInteger _count = new AtomicInteger();
        private final AtomicLongArray _values = new AtomicLongArray(1024);

        UniformDoubleReservoir() {
        }

        public void sample(double n) {
            int cnt = _count.incrementAndGet();
            if (cnt <= 1024) {
                _values.set(cnt-1, Double.doubleToLongBits(n));
            } else {
                int idx = _randoms.get().nextInt(cnt);
                if (idx < 1024) {
                    _values.set(idx, Double.doubleToLongBits(n));
                }
            }
        }

        public double[] toArray() {
            int cnt = Math.min(1024, _count.get());

            double[] vals = new double[cnt];
            for (int i = 0; i < cnt; i++) {
                vals[i] = Double.longBitsToDouble(_values.get(i));
            }
            Arrays.sort(vals);

            return vals;
        }
    }

    private final int _numWorkers;
    private final double[] _utilizations;
    private final double[] _taskArrivalRates;
    private final double[] _taskCompletionRates;
    private final double[] _taskRejectionRates;
    private final long[] _queueLengths;
    private final long[] _queueLatencies;
    private final long[] _taskLatencies;

    public static Stats EMPTY = new Stats(0, new double[] {}, new double[] {}, new double[] {}, new double[] {}, new long[] {}, new long[] {}, new long[] {});

    public Stats(int numWorkers, double[] utilizations, double[] taskArrivalRates, double[] taskCompletionRates, double[] taskRejectionRates, long[] queueLengths, long[] queueLatencies, long[] taskLatencies) {
        _numWorkers = numWorkers;
        _utilizations = utilizations;
        _taskArrivalRates = taskArrivalRates;
        _taskCompletionRates = taskCompletionRates;
        _taskRejectionRates = taskRejectionRates;
        _queueLengths = queueLengths;
        _queueLatencies = queueLatencies;
        _taskLatencies = taskLatencies;
    }

    ///

    private static double lerp(long low, long high, double t) {
        return low + (high - low) * t;
    }

    private static double lerp(double low, double high, double t) {
        return low + (high - low) * t;
    }

    private static double lerp(long[] vals, double t) {

        if (t < 0 || 1 < t) {
            throw new IllegalArgumentException(new Double(t).toString());
        }

        int cnt = vals.length;

        switch (cnt) {
        case 0:
            return 0.0;
        case 1:
            return (double) vals[0];
        default:
            if (t == 1.0) {
                return (double) vals[cnt-1];
            }
            double idx = (cnt-1) * t;
            int iidx = (int) idx;
            return lerp(vals[iidx], vals[iidx + 1], idx - iidx);
        }
    }

    private static double lerp(double[] vals, double t) {

        if (t < 0 || 1 < t) {
            throw new IllegalArgumentException(new Double(t).toString());
        }

        int cnt = vals.length;

        switch (cnt) {
        case 0:
            return 0.0;
        case 1:
            return (double) vals[0];
        default:
            if (t == 1.0) {
                return (double) vals[cnt-1];
            }
            double idx = (cnt-1) * t;
            int iidx = (int) idx;
            return lerp(vals[iidx], vals[iidx + 1], idx - iidx);
        }
    }

    private static double mean(double[] vals) {
        double sum = 0;
        for (int i = 0; i < vals.length; i++) {
            sum += vals[i];
        }
        return sum/vals.length;
    }

    private static double mean(long[] vals) {
        long sum = 0;
        for (int i = 0; i < vals.length; i++) {
            sum += vals[i];
        }
        return sum/vals.length;
    }

    ///

    /**
     * @return the number of active workers in the pool.
     */
    public int getNumWorkers() {
        return _numWorkers;
    }

    /**
     * @return the mean utilization of the workers as a value between 0 and 1.
     */
    public double getMeanUtilization() {
        return mean(_utilizations);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the utilization of the workers as a value between 0 and 1
     */
    public double getUtilization(double quantile) {
        return lerp(_utilizations, quantile);
    }

    /**
     * @return the mean task arrival rate of the executor, in tasks per second
     */
    public double getMeanTaskArrivalRate() {
        return mean(_taskArrivalRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task arrival rate of the executor, in tasks per second
     */
    public double getTaskArrivalRate(double quantile) {
        return lerp(_taskArrivalRates, quantile);
    }

    /**
     * @return the mean task completion rate of the executor, in tasks per second
     */
    public double getMeanTaskCompletionRate() {
        return mean(_taskCompletionRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task completion rate of the executor, in tasks per second
     */
    public double getTaskCompletionRate(double quantile) {
        return lerp(_taskCompletionRates, quantile);
    }

    /**
     * @return the mean task rejection rate of the executor, in tasks per second
     */
    public double getMeanTaskRejectionRate() {
        return mean(_taskRejectionRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task rejection rate of the executor, in tasks per second
     */
    public double getTaskRejectionRate(double quantile) {
        return lerp(_taskRejectionRates, quantile);
    }

    /**
     * @return the mean length of the queue
     */
    public double getMeanQueueLength() {
        return mean(_queueLengths);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the length of the queue
     */
    public double getQueueLength(double quantile) {
        return lerp(_queueLengths, quantile);
    }

    /**
     * @return the mean time each task spends on the queue, in nanoseconds
     */
    public double getMeanQueueLatency() {
        return mean(_queueLatencies);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the time each task spends on the queue, in nanoseconds
     */
    public double getQueueLatency(double quantile) {
        return lerp(_queueLatencies, quantile);
    }

    /**
     * @return the mean time each task takes to complete, including time on the queue, in nanoseconds
     */
    public double getMeanTaskLatency() {
        return mean(_taskLatencies);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the time each task takes to complete, including time on the queue, in nanoseconds
     */
    public double getTaskLatency(double quantile) {
        return lerp(_taskLatencies, quantile);
    }
}
