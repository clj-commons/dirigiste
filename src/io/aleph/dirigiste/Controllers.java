package io.aleph.dirigiste;

public class Controllers {

    public static Controller utilization(final double targetUtilization, final int maxThreadCount) {
        return new Controller() {
            public boolean shouldIncrement(int numWorkers) {
                return numWorkers < maxThreadCount;
            }

            public int adjustment(Stats stats) {
                double utilization = stats.getUtilization(0.9) / targetUtilization;
                return (int) (stats.getWorkerCount() * utilization) - stats.getWorkerCount();
            }
        };
    }

}
