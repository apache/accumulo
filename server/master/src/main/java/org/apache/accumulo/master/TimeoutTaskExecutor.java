package org.apache.accumulo.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Runs one or more tasks with a timeout per task (instead of a timeout for the entire pool). Uses callbacks to invoke functions on successful, timed out, or
 * tasks that error.
 *
 * @param <T>
 *          The return type for the corresponding Callable.
 * @param <C>
 *          The type of Callable submitted to this executor.
 */
public class TimeoutTaskExecutor<T,C extends Callable<T>> implements AutoCloseable {

  private final static Logger log = LoggerFactory.getLogger(TimeoutTaskExecutor.class);

  private final long timeout;
  private final ExecutorService executorService;
  private final List<WrappedTask> wrappedTasks;

  private SuccessCallback<T,C> successCallback;
  private ExceptionCallback<C> exceptionCallback;
  private TimeoutCallback<C> timeoutCallback;

  /**
   * Constructs a new TimeoutTaskExecutor using the given executor to schedule tasks with a max timeout. Takes an expected number of Callables to initialize the
   * underlying task collection more appropriately.
   *
   * @param executorService
   *          The executor to schedule tasks with.
   * @param timeout
   *          The timeout for each task.
   * @param expectedNumCallables
   *          The expected number of callables you will schedule (for sizing optimization).
   */
  public TimeoutTaskExecutor(ExecutorService executorService, long timeout, int expectedNumCallables) {
    this.executorService = executorService;
    this.timeout = timeout;
    this.wrappedTasks = new ArrayList<>(expectedNumCallables);
  }

  /**
   * Submits a new task to the executor.
   *
   * @param callable
   *          Task to run
   */
  public void submit(C callable) {
    WrappedTask wt = new WrappedTask(callable, executorService.submit(callable));
    wrappedTasks.add(wt);
  }

  /**
   * Registers the callback to use on successful tasks.
   *
   * @param successCallback
   *          The callback function to invoke on success.
   * @throws NullPointerException
   *           when a null successCallback is passed in
   */
  public void onSuccess(SuccessCallback<T,C> successCallback) {
    this.successCallback = Objects.requireNonNull(successCallback, "Must provide a non-null successCallback.");
  }

  /**
   * Registers the callback to use on tasks that throw exceptions.
   *
   * @param exceptionCallback
   *          The callback function to invoke on exceptions.
   * @throws NullPointerException
   *           when a null exceptionCallback is passed in
   */
  public void onException(ExceptionCallback<C> exceptionCallback) {
    this.exceptionCallback = Objects.requireNonNull(exceptionCallback, "Must provide a non-null exceptionCallback.");
  }

  /**
   * Registers the callback to use on tasks that time out.
   *
   * @param timeoutCallback
   *          The callback function to invoke on timeouts.
   * @throws NullPointerException
   *           when a null timeoutCallback is passed in
   */
  public void onTimeout(TimeoutCallback<C> timeoutCallback) {
    this.timeoutCallback = Objects.requireNonNull(timeoutCallback, "Must provide a non-null timeoutCallback.");
  }

  /**
   * Completes all the current tasks by dispatching to the appropriated callback.
   *
   * @throws IllegalStateException
   *           If all of the callbacks were not registered before calling this method.
   * @throws InterruptedException
   *           If interrupted while awaiting callable results.
   */
  public void complete() throws InterruptedException {
    Preconditions.checkState(successCallback != null, "Must set a success callback before completing " + this);
    Preconditions.checkState(exceptionCallback != null, "Must set an exception callback before completing " + this);
    Preconditions.checkState(timeoutCallback != null, "Must set a timeout callback before completing " + this);

    for (WrappedTask wt : wrappedTasks) {
      try {
        successCallback.accept(wt.callable, wt.future.get(timeout, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
        throw e;
      } catch (TimeoutException e) {
        wt.future.cancel(true);
        timeoutCallback.accept(wt.callable);
      } catch (Exception e) {
        exceptionCallback.accept(wt.callable, e);
      }
    }
  }

  @Override
  public void close() {
    try {
      executorService.shutdownNow();
    } catch (Exception e) {
      log.warn("Error while shutting down " + this, e);
    }
  }

  /*
   * A wrapper for keeping a callable and its future together.
   */
  private class WrappedTask {
    public final C callable;
    public final Future<T> future;

    public WrappedTask(C callable, Future<T> future) {
      this.callable = callable;
      this.future = future;
    }
  }

  /**
   * Callback interface for a task that was successful.
   *
   * @param <T>
   *          The result of the Callable
   * @param <C>
   *          The Callable
   */
  public interface SuccessCallback<T,C> {
    void accept(C task, T result);
  }

  /**
   * Callback interface for a task that threw an Exception.
   *
   * @param <C>
   *          The Callable
   */
  public interface ExceptionCallback<C> {
    void accept(C task, Exception e);
  }

  /**
   * Callback interface for a task that timed out.
   *
   * @param <C>
   *          The Callable
   */
  public interface TimeoutCallback<C> {
    void accept(C task);
  }
}
