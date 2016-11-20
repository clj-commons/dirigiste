![](docs/cybersyn.jpg)

(pronounced deer-eh-jeest)

In the default JVM thread pools, once a thread is created it will only be retired when it hasn't performed a task in the last minute.  In practice, this means that there are as many threads as the peak historical number of concurrent tasks handled by the pool, forever.  These thread pools are also poorly instrumented, making it difficult to tune their latency or throughput.

Dirigiste provides a fast, richly instrumented version of a `java.util.concurrent.ExecutorService`, and provides a means to feed that instrumentation into a control mechanism that can grow or shrink the pool as needed.  Default implementations that optimize the pool size for thread utilization are provided.

It also provides an object pool mechanism that uses a similar feedback mechanism to resize itself, and is significantly simpler than the [Apache Commons object pool implementation](http://commons.apache.org/proper/commons-pool/).

Full documentation can be found [here](http://ztellman.github.com/dirigiste/).

### usage

In Leiningen:

```clj
[io.aleph/dirigiste "0.1.4"]
```

In Maven:

```xml
<dependency>
  <groupId>io.aleph</groupId>
  <artifactId>dirigiste</artifactId>
  <version>0.1.4</version>
</dependency>
```

### executors

Using the default utilization executor is simple, via [`Executors.utilizationExecutor(...)`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executors.html#utilization\(double,%20int\)):

```java
import io.aleph.dirigiste.Executors;

...

ExecutorService e = Executors.utilizationExecutor(0.9, 64);
```

This will create an executor which will try to size the pool such that 90% of the threads are active, but will not grow beyond 64 threads.

This executor exposes [`getStats`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executor.html#getStats\(\)) and [`getLastStats`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executor.html#getLastStats\(\)) methods, which can be used to examine the performance characteristics of the executor.  `getLastStats` uses the last value passed to the control loop, so can return immediately.  `getStats` returns the statistics gathered since the last control update, and so may contain 0 or more samples, and requires some amount of computation.

Since instrumentation will cause some small overhead, you may specify which dimensions you wish to collect, via the [`Metric`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executor.Metric.html) class.  The possible fields are as follows:

| metric | description |
|-------|-------------|
| `QUEUE_LATENCY` | the time spent on the queue for each task, in nanoseconds |
| `TASK_LATENCY` | the time taken to complete a task, including time spent on the queue, in nanoseconds |
| `QUEUE_LENGTH` | the length of the queue |
| `TASK_ARRIVAL_RATE` | the rate of incoming tasks per second |
| `TASK_COMPLETION_RATE` | the rate of completed tasks per second |
| `TASK_REJECTION_RATE` | the rate of rejected tasks per second |
| `UTILIZATION` | the portion of threads which are active, from 0 to 1 |

These metrics are surfaced via the [`Stats`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Stats.html) class, which provides `getMean...()` and `get...(double quantile)` for each metric.  By default, the utilization executor will only measure utilization, but if we want to get the full range of metrics, we can instantiate it like this:

```java
Executors.utilizationExecutor(0.9, 64, EnumSet.allOf(Executor.Metric));
```

This will allow us to track metrics which aren't required for the control loop, but are useful elsewhere.

### pools

All pools are defined via their generator, which is used to create and destroy the pooled objects:

```java
public interface Pool.Generator<K,V> {
  V generate(K key) throws Exception;
  void destroy(K key, V val);
}
```

All pooled objects have an associated key.  If objects have no external resources that must be explicitly disposed, `destroy` can be a no-op.

Object pools have three major functions, `acquire`, `release`, and `dispose`.  Typically, objects will be taken out of the pool via `acquire`, and returned back via `release` once they've served their purpose:

```java
pool = Pools.utilizationPool(generator, 0.9, 4, 1024);
Object obj = pool.acquire("foo");
useObject(obj);
pool.release("foo", obj);
```

However, if the object has expired, we can `dispose` of it:

```java
pool.dispose("foo", obj);
```

A pooled object can be disposed of at any time, without having first been acquired.

To support non-blocking code, we may also acquire an object via a callback mechanism:

```java
pool.acquire("foo",
  new AcquireCallback() {
        public void handleObject(Object obj) {
                useObject(obj);
                pool.release("foo", obj);
        }
});
```

### creating a custom controller

The [`Executor.Controller`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executor.Controller.html) interface is fairly straightforward:

```java
public interface Executor.Controller {
    boolean shouldIncrement(int currThreads);
    int adjustment(Stats stats);
}
```

The first method, `shouldIncrement`, controls whether a new thread should be spun up.  This means that the thread limit can be dynamic, for instance dependent on the available memory.  This method will be called whenever `adjustment` calls for more threads, or when a task is unable to be added to the queue.

The second method, `adjustment`, takes a `Stats` object, and returns a number representing how the pool size should be adjusted.  The frequency with which `adjustment` is called is dictated by the `controlPeriod` parameter to the [`Executor`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Executor.html#Executor\(java.util.concurrent.ThreadFactory,%20java.util.concurrent.BlockingQueue,%20io.aleph.dirigiste.Controller,%20java.util.EnumSet,%20long,%20long,%20java.util.concurrent.TimeUnit\)) constructor, and the number of samples in the `Stats` object is controlled by the `samplePeriod` parameter.

The utilization controller is quite simple:

```java
Executor.Controller utilizationController(final double targetUtilization, final int maxThreadCount) {
  return new Controller() {
    public boolean shouldIncrement(int numWorkers) {
      return numWorkers < maxThreadCount;
    }

    public int adjustment(Stats stats) {
      double correction = stats.getUtilization(0.9) / targetUtilization;
      return (int) Math.ceil(stats.getNumWorkers() * correction) - stats.getNumWorkers();
    }
  };
}
```

It adjusts the number of threads using the `targetUtilization` compared against the 90th percentile measured utilization over the last `controlPeriod`.  Obviously more sophisticated methods are possible, but they're left as an exercise for the reader.

[`Pool.Controller`](http://ztellman.github.com/dirigiste/io/aleph/dirigiste/Pool.Controller.html) works much the same, except that `adjustment` takes a `Map` of keys onto `Stats` objects, and returns a `Map` of keys onto `Integer` objects.  The utilization controller is otherwise much the same:

```java
public Pool.Controller utilizationController(final double targetUtilization, final int maxObjectsPerKey, final int maxTotalObjects) {

  return new Pool.Controller() {
    public boolean shouldIncrement(Objec t key, int objectsForKey, int totalObjects) {
      return (objectsForKey < maxObjectsPerKey) && (totalObjects < maxTotalObjects);
    }

    public Map adjustment(Map<K, Stats> stats) {
      Map adj = new HashMap();

      for ( e : stats.entrySet()) {
        Map.Entry entry = (Map.Entry) e;
        Stats s = (Stats) entry.getValue();
        int numWorkers = s.getNumWorkers();
        double correction = s.getUtilization(0.9) / targetUtilization;
        int n = (int) Math.ceil(s.getNumWorkers() * correction) - numWorkers;

        adj.put(entry.getKey(), new Integer(n));
      }

      return adj;
    }
  };
}
```

### license

Copyright © 2015 Zachary Tellman

Distributed under the MIT License
