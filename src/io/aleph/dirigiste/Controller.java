package io.aleph.dirigiste;

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
