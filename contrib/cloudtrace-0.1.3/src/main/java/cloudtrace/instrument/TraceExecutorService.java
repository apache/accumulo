package cloudtrace.instrument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TraceExecutorService implements ExecutorService {

    private final ExecutorService impl;
    
    public TraceExecutorService(ExecutorService impl) {
        this.impl = impl;
    }
    
    @Override
    public void execute(Runnable command) {
        impl.execute(new TraceRunnable(command));
    }

    @Override
    public void shutdown() {
        impl.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return impl.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return impl.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return impl.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return impl.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return submit(new TraceCallable<T>(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return submit(new TraceRunnable(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return submit(new TraceRunnable(task));
    }
    
    private <T> Collection<? extends Callable<T>> wrapCollection(Collection<? extends Callable<T>> tasks) {
        List<Callable<T>> result = new ArrayList<Callable<T>>();
        for (Callable<T> task : tasks) {
            result.add(new TraceCallable<T>(task));
        }
        return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return invokeAll(wrapCollection(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return invokeAll(wrapCollection(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
       return invokeAny(wrapCollection(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(wrapCollection(tasks), timeout, unit);
    }

}
