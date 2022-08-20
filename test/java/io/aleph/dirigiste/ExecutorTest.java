package io.aleph.dirigiste;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ExecutorTest {
    @Test
    public void testAwaitTerminationShouldReturnFalse() throws InterruptedException {
        Executor executor = Executors.fixedExecutor(1);
        executor.executeWithoutRejection(() -> {});
        executor.shutdown();
        boolean result = executor.awaitTermination(500, TimeUnit.MICROSECONDS);
        assertFalse(result);
    }

    @Test
    public void testAwaitTerminationShouldReturnTrue() throws InterruptedException {
        Executor executor = Executors.fixedExecutor(1);
        executor.executeWithoutRejection(() -> {});
        executor.shutdown();
        boolean result = executor.awaitTermination(1200, TimeUnit.MICROSECONDS);
        assertTrue(result);
    }
}
