package io.aleph.dirigiste;

public class Controllers {

    public static Controller utilization(final double targetUtilization, final int maxThreadCount) {
        return new Controller() {
            public boolean shouldIncrement(int numWorkers) {
                return numWorkers < maxThreadCount;
            }

            public int adjustment(Stats stats) {
                int numWorkers = stats.getNumWorkers();
                double correction = stats.getUtilization(0.9) / targetUtilization;
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
