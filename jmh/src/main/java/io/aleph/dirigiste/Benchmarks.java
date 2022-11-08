package io.aleph.dirigiste;

import io.aleph.dirigiste.IPool.Controller;
import io.aleph.dirigiste.IPool.Generator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 5)
@Fork(1)
public class Benchmarks {

  private final Controller<Integer> controller = Pools.utilizationController(0.9, 8, 1024);
  private Generator<Integer,UUID> generator;

  private Pool<Integer, UUID> pool;

  @Setup
  public void setup() {
    generator = new Generator<Integer,UUID>() {
      @Override
      public UUID generate(Integer key) {
        return UUID.randomUUID();
      }

      @Override
      public void destroy(Integer key, UUID val) {
      }
    };
    pool = new Pool<>(generator, controller, 65536, 1, 100, TimeUnit.MICROSECONDS);
  }

  @Benchmark
  @Measurement(batchSize = 1, iterations = 3)
  @Threads(1)
  public void thread1_batchSize1() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 1, iterations = 3)
  @Threads(5)
  public void thread5_batchSize1() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 1, iterations = 3)
  @Threads(20)
  public void thread20_batchSize1() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 1, iterations = 3)
  @Threads(100)
  public void thread100_batchSize1() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 100, iterations = 3)
  @Threads(1)
  public void thread1_batchSize100() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 100, iterations = 3)
  @Threads(5)
  public void thread5_batchSize100() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 100, iterations = 3)
  @Threads(20)
  public void thread20_batchSize100() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }

  @Benchmark
  @Measurement(batchSize = 100, iterations = 3)
  @Threads(100)
  public void thread100_batchSize100() throws InterruptedException {
    UUID value = pool.acquire(1);
    pool.dispose(1,value);
  }
}
