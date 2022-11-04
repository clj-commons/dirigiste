package io.aleph.dirigiste;

import io.aleph.dirigiste.IPool.Controller;
import io.aleph.dirigiste.IPool.Generator;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PoolTest {

    private static final Key KEY = new Key("foo");

    @Test
    public void testPoolWithOneAcquire() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        pool.acquire(KEY);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(1, pool.queue(KEY).objects.get());
        assertEquals(1, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithOneAcquireOneReleaseOneAcquire() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        Value val = pool.acquire(KEY);
        pool.release(KEY, val);
        pool.acquire(KEY);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(1, pool.queue(KEY).objects.get());
        assertEquals(1, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithThreeAcquire() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        pool.acquire(KEY);
        pool.acquire(KEY);
        pool.acquire(KEY);
        Thread.sleep(500);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(3, pool.queue(KEY).objects.get());
        assertEquals(1, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithTwoAcquireOneDispose() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        pool.acquire(KEY);
        Value val = pool.acquire(KEY);
        pool.dispose(KEY, val);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(1, pool.queue(KEY).objects.get());
        assertEquals(1, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithOneAcquireOneDispose() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        Value val = pool.acquire(KEY);
        pool.dispose(KEY, val);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(0, pool.queue(KEY).objects.get());
        assertEquals(0, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithTwoAcquireOneRelease() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        pool.acquire(KEY);
        Value val = pool.acquire(KEY);
        pool.release(KEY, val);
        assertEquals(1, pool.queue(KEY).availableObjectsCount());
        assertEquals(2, pool.queue(KEY).objects.get());
        assertEquals(0.5, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithTwoAcquireTwoRelease() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        Value val = pool.acquire(KEY);
        Value val2 = pool.acquire(KEY);
        pool.release(KEY, val);
        pool.release(KEY, val2);
        assertEquals(2, pool.queue(KEY).availableObjectsCount());
        assertEquals(2, pool.queue(KEY).objects.get());
        assertEquals(0, getUtilization(pool), 0);
    }

    @Test
    public void testPoolWithTwoAcquireOneReleaseOneDispose() throws InterruptedException {
        Pool<Key,Value> pool = newPool();
        Value val = pool.acquire(KEY);
        Value val2 = pool.acquire(KEY);
        pool.release(KEY, val);
        pool.dispose(KEY, val2);
        assertEquals(1, pool.queue(KEY).availableObjectsCount());
        assertEquals(1, pool.queue(KEY).objects.get());
        assertEquals(0, getUtilization(pool), 0);
    }

    @Test
    public void testFullPoolWithOneAcquire() {
        Pool<Key,Value> pool = newPool(fullController());
        pool.acquire(KEY, __ -> {});
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(0, pool.queue(KEY).objects.get());
        assertEquals(2, getUtilization(pool), 0);
    }

    @Test
    public void testFullPoolWithTwoAcquire() throws InterruptedException {
        Pool<Key,Value> pool = newPool(fullController());
        pool.acquire(KEY, __ -> {});
        pool.acquire(KEY, __ -> {});
        Thread.sleep(200);
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(0, pool.queue(KEY).objects.get());
        assertEquals(3, getUtilization(pool), 0);
    }

    @Test
    public void testFullPoolWithThreeAcquire() throws InterruptedException {
        // This test is performed multiple times as it detected some race conditions
        // to ensure the queues are not removed while in use.
        for(int i=0;i<100;i++) {
            Pool<Key, Value> pool = newPool(fullController());
            pool.acquire(KEY, __ -> {});
            pool.acquire(KEY, __ -> {});
            pool.acquire(KEY, __ -> {});
            Thread.sleep(50);
            assertEquals(0, pool.queue(KEY).availableObjectsCount());
            assertEquals(0, pool.queue(KEY).objects.get());
            assertEquals(4, getUtilization(pool), 0);
        }
    }

    @Test
    public void testPoolWithMisbehavingGenerator() {
        Pool<Key,Value> pool = newPool(noopController(), misbehavingGenerator());
        assertThrows(Exception.class, () -> pool.acquire(KEY));
    }

    @Test
    public void testPoolWithMisbehavingDestroyGenerator() throws InterruptedException {
        Pool<Key,Value> pool = newPool(noopController(), misbehavingDestroyGenerator());
        Value val = pool.acquire(KEY);
        assertThrows(Exception.class, () -> pool.dispose(KEY, val));
    }

    @Test
    public void testPoolDisposeWhenShutdown() throws InterruptedException {
        Pool<Key,Value> pool = newPool(noopController(), misbehavingDestroyGenerator());
        Value val = pool.acquire(KEY);
        pool.shutdown();
        assertThrows(Exception.class, () -> pool.dispose(KEY, val));
    }

    @Test
    public void testPoolAcquireWhenQueueFull() {
        Pool<Key,Value> pool = newPool(fullController(), generator(), 1);
        pool.acquire(KEY, __ -> {});
        assertThrows(RejectedExecutionException.class, () -> pool.acquire(KEY));
    }

    @Test
    public void testPoolShutdown() throws InterruptedException {
        Pool<Key,Value> pool = newPool(noopController());
        Value val = pool.acquire(KEY);
        pool.release(KEY, val);
        pool.shutdown();
        assertEquals(0, pool.queue(KEY).availableObjectsCount());
        assertEquals(0, pool.queue(KEY).objects.get());
        assertEquals(0, getUtilization(pool), 0);
    }

    @Test
    public void testPoolAcquireOnShutdown() throws InterruptedException {
        Pool<Key,Value> pool = newPool(noopController());
        Value val = pool.acquire(KEY);
        pool.release(KEY, val);
        pool.shutdown();
        assertThrows(Exception.class, () -> pool.acquire(KEY));
    }

    @Test
    public void testPoolQueueRemovalWhenNotInUseWithRelease() throws InterruptedException {
        Pool<Key, Value> pool = newPool(utilizationController());
        Value val = pool.acquire(KEY);
        pool.release(KEY, val);
        // Wait for the controlPeriod
        Thread.sleep(300);
        assertNull(pool._queues.get(KEY));
    }

    @Test
    public void testPoolQueueRemovalWhenNotInUseWithDispose() throws InterruptedException {
        Pool<Key,Value> pool = newPool(utilizationController());
        Value val = pool.acquire(KEY);
        pool.dispose(KEY, val);
        // Wait for the controlPeriod
        Thread.sleep(300);
        assertNull(pool._queues.get(KEY));
    }

    @Test
    public void testPoolWithSimpleUtilizationExecutor() throws InterruptedException {
        Pool<Key,Value> pool = newPool(utilizationController());
        Value val1 = pool.acquire(KEY);
        Value val2 = pool.acquire(KEY);
        pool.release(KEY, val1);
        pool.dispose(KEY, val2);
        // Wait for the controlPeriod
        Thread.sleep(300);
        assertNull(pool._queues.get(KEY));
    }

    @Test
    public void testPoolOnAHighlyConcurrentEnvironment() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(30);
        Pool<Key,Value> pool = newPool(utilizationController());

        List<Future<Boolean>> futures = IntStream.range(0, 1000).mapToObj(__ -> executorService.submit(() -> {
            try {
                Value val = pool.acquire(KEY);
                pool.dispose(KEY, val);
                return true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).collect(Collectors.toList());

        assertTrue(futures.stream().allMatch(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        int available = pool.queue(KEY).availableObjectsCount();
        int objects = pool.queue(KEY).objects.get();
        double utilization = getUtilization(pool);

        // We should have between 0 and 1000 objects whether they haven't
        // be destroyed yet
        assertTrue(available >= 0 && available <= 1000);
        assertTrue(objects >= 0 && objects <= 1000);
        assertTrue(utilization >= 0 && utilization <= 1);

        // Wait for the controlPeriod
        Thread.sleep(300);
        assertNull(pool._queues.get(KEY));
    }

    @Test
    public void testPoolOnASmallConcurrentEnvironment() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(30);
        Pool<Key,Value> pool = newPool(utilizationController());

        List<Future<Boolean>> futures = IntStream.range(0, 5).mapToObj(__ -> executorService.submit(() -> {
            try {
                for(int i = 0;i<=100000;i++) {
                    Value val = pool.acquire(KEY);
                    pool.dispose(KEY, val);
                }
                return true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).collect(Collectors.toList());

        assertTrue(futures.stream().allMatch(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        int available = pool.queue(KEY).availableObjectsCount();
        int objects = pool.queue(KEY).objects.get();
        double utilization = getUtilization(pool);

        // We should have between 0 and 5 objects whether they haven't
        // be destroyed yet
        assertTrue(available >= 0 && available <= 5);
        assertTrue(objects >= 0 && objects <= 5);
        assertTrue(utilization >= 0 && utilization <= 1);

        // Wait for the controlPeriod
        Thread.sleep(300);
        assertNull(pool._queues.get(KEY));
    }

    @Test
    public void testPoolAcquireReleaseMultipleTimes() {
        Pool<Key,Value> pool = newPool(utilizationController());
        assertTrue(IntStream.range(0, 10000).mapToObj(i -> {
            try {
                Value val = pool.acquire(KEY);
                pool.dispose(KEY, val);
                return true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).allMatch(v -> v));
    }

    private Pool<Key, Value> newPool() {
        return newPool(noopController(), generator());
    }

    private Pool<Key, Value> newPool(Controller<Key> controller) {
        return new Pool<>(generator(), controller, 65536, 1, 1, TimeUnit.MICROSECONDS);
    }

    private Pool<Key, Value> newPool(Controller<Key> controller, Generator<Key, Value> generator) {
        return new Pool<>(generator, controller, 65536, 10, 1000, TimeUnit.MICROSECONDS);
    }

    private Pool<Key, Value> newPool(Controller<Key> controller, Generator<Key, Value> generator, int maxQueueSize) {
        return new Pool<>(generator, controller, maxQueueSize, 10, 1000, TimeUnit.MICROSECONDS);
    }

    private double getUtilization(Pool<Key, Value> pool) {
        return pool.getUtilization(pool.queue(KEY).availableObjectsCount(), pool.queue(KEY).getQueueLength(), pool.queue(KEY).objects.get());
    }

    private Generator<Key,Value> generator() {
        return new Generator<Key,Value>() {
            @Override
            public Value generate(Key key) {
                return new Value(UUID.randomUUID().toString());
            }

            @Override
            public void destroy(Key key, Value val) {
                // Nothing to clean up
            }
        };
    }

    private Generator<Key,Value> misbehavingGenerator() {
        return new Generator<Key,Value>() {
            @Override
            public Value generate(Key key) {
                throw new RuntimeException("BOOM!");
            }

            @Override
            public void destroy(Key key, Value val) {
                throw new RuntimeException("BOOM!");
            }
        };
    }

    private Generator<Key,Value> misbehavingDestroyGenerator() {
        return new Generator<Key,Value>() {
            @Override
            public Value generate(Key key) {
                return new Value(UUID.randomUUID().toString());
            }

            @Override
            public void destroy(Key key, Value val) {
                throw new RuntimeException("BOOM!");
            }
        };
    }

    private Controller<Key> noopController() {
        return new Controller<Key>() {
            @Override
            public boolean shouldIncrement(Key key, int objectsForKey, int totalObjects) {
                return true;
            }

            @Override
            public Map<Key, Integer> adjustment(Map<Key, Stats> stats) {
                return stats.entrySet().stream().collect(toMap(Map.Entry::getKey, __ -> 1));
            }
        };
    }

    private Controller<Key> fullController() {
        return new Controller<Key>() {
            @Override
            public boolean shouldIncrement(Key key, int objectsForKey, int totalObjects) {
                return false;
            }

            @Override
            public Map<Key, Integer> adjustment(Map<Key, Stats> stats) {
                return stats.entrySet().stream().collect(toMap(Map.Entry::getKey, __ -> 0));
            }
        };
    }

    private Controller<Key> utilizationController() {
        return Pools.utilizationController(0.9, 8, 1024);
    }

    // An identity-based key
    private static class Key {
        public final String value;

        Key(String value) {
            this.value = value;
        }
    }

    // An identity-based value
    private static class Value {
        public final String value;

        Value(String value) {
            this.value = value;
        }
    }
}
