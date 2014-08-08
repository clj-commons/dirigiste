package dirigiste;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

public class DirigisteExecutorService extends AbstractExecutorService {

    public interface IExecutorService {
        boolean awaitTermination(long timeout);
        void execute(Runnable runnable);
        boolean isShutdown();
        boolean isTerminated();
        void shutdown();
        List<Runnable> shutdownNow();
    }

    private final IExecutorService _service;

    public DirigisteExecutorService(IExecutorService service) {
        _service = service;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return _service.awaitTermination(unit.toMillis(timeout));
    }

    @Override
    public void execute(Runnable runnable) {
        _service.execute(runnable);
    }

    @Override
    public boolean isShutdown() {
        return _service.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return _service.isTerminated();
    }

    @Override
    public void shutdown() {
        _service.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return _service.shutdownNow();
    }

}
