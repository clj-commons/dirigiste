package io.aleph.dirigiste;

import java.util.Map;

public interface IPool<K,V> {

    interface Controller<K> {

        /**
         * @param key  the key which requires a new object
         * @param objectsForKey  the number of currently existing objects for 'key'
         * @param totalObjects  the total number of objects across every key
         * @return 'true' if a new object under 'key' should be created, false otherwise
         */
        boolean shouldIncrement(K key, int objectsForKey, int totalObjects);

        /**
         * @param stats  a map of key onto stats for that key
         * @return a map of key onto how many objects should be created (if positive) or disposed (if negative)
         */
        Map<K,Integer> adjustment(Map<K,Stats> stats);
    }

    interface Generator<K,V> {
        /**
         * Creates a new instance of the pooled object, which must be
         * non-null, and non-equal to all other generated objects.
         *
         * @param key  the key for which the object is being generated.
         */
        V generate(K key) throws Exception;

        /**
         * Disposes of the generated value.  Should be idempotent.
         *
         * @param val  an object which was previously created via 'generate'.
         */
        void destroy(K key, V val);
    }

    interface AcquireCallback<V> {

        /**
         * A callback that returns a pooled object.
         */
        void handleObject(V obj);
    }

    /**
     * Acquires an object from the pool, potentially creating one if none is available.
     *
     * @param key  the key of the pooled object being acquired
     * @param callback  the callback that will be invoked with the object once it's available
     */
    void acquire(K key, AcquireCallback<V> callback);

    /**
     * Acquires an object from the pool, potentially creating one if none is available.
     *
     * @param key  the key of the pooled object being acquired
     * @return the object, once it's acquired
     */
    V acquire(K key) throws InterruptedException;

    /**
     * Releases an object that has been acquired back to the pool.
     *
     * @param key  the key of the pooled object being released
     * @param obj  the pooled object being released
     */
    void release(K key, V obj);

    /**
     * Disposes of an object, removing it from the pool.
     *
     * @param key  the key of the pooled object being disposed
     * @param obj  the pooled object being disposed
     */
    void dispose(K key, V obj);

    void shutdown();
}
