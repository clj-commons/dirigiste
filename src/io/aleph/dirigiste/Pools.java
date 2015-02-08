package io.aleph.dirigiste;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Pools {

    /**
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool.Controller fixedController(final int maxObjectsPerKey, final int maxTotalObjects) {
        return new IPool.Controller() {
            public boolean shouldIncrement(Object key, int objectsForKey, int totalObjects) {
                return (objectsForKey < maxObjectsPerKey) && (totalObjects < maxTotalObjects);
            }

            public Map adjustment(Map stats) {
                return new HashMap();
            }
        };
    }

    /**
     * @param targetUtilization the target utilization per key, within [0, 1]
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool.Controller utilizationController(final double targetUtilization, final int maxObjectsPerKey, final int maxTotalObjects) {

        return new IPool.Controller() {
            public boolean shouldIncrement(Object key, int objectsForKey, int totalObjects) {
                return (objectsForKey < maxObjectsPerKey) && (totalObjects < maxTotalObjects);
            }

            public Map adjustment(Map stats) {
                Map adj = new HashMap();

                for (Object e : stats.entrySet()) {
                    Map.Entry entry = (Map.Entry) e;
                    Stats s = (Stats) entry.getValue();
                    int numWorkers = s.getNumWorkers();
                    double correction = s.getUtilization(1.0) / targetUtilization;
                    int n = (int) Math.ceil(s.getNumWorkers() * correction) - numWorkers;

                    adj.put(entry.getKey(), new Integer(n));
                }
                return adj;
            }
        };
    }

    /**
     * @param generator the pooled object generator
     * @param targetUtilization the target utilization per key, within [0, 1]
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool utilizationPool(IPool.Generator generator, double targetUtilization, int maxObjectsPerKey, int maxTotalObjects) {
        return utilizationPool(generator, 65536, targetUtilization, maxObjectsPerKey, maxTotalObjects);
    }

    /**
     * @param generator the pooled object generator
     * @param maxQueueLength the maximum number of acquire requests that can be queued
     * @param targetUtilization the target utilization per key, within [0, 1]
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool utilizationPool(IPool.Generator generator, int maxQueueLength, double targetUtilization, int maxObjectsPerKey, int maxTotalObjects) {
        return new Pool(generator, utilizationController(targetUtilization, maxObjectsPerKey, maxTotalObjects), maxQueueLength, 25, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * @param generator the pooled object generator
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool fixedPool(IPool.Generator generator, int maxObjectsPerKey, int maxTotalObjects) {
        return fixedPool(generator, 65536, maxObjectsPerKey, maxTotalObjects);
    }

    /**
     * @param generator the pooled object generator
     * @param maxQueueLength the maximum number of acquire requests that can be queued
     * @param maxObjectsPerKey the maximum number of pooled objects per key
     * @param maxTotalObjects the total number of object that the pool can contain
     */
    public static IPool fixedPool(IPool.Generator generator, int maxQueueLength, int maxObjectsPerKey, int maxTotalObjects) {
        return new Pool(generator, fixedController(maxObjectsPerKey, maxTotalObjects), maxQueueLength, 25, 1000, TimeUnit.MILLISECONDS);
    }
}
