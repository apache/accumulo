/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Runs one or more tasks with a timeout per task (instead of a timeout for the entire pool). Uses
 * callbacks to invoke functions on successful, timed out, or tasks that error.
 *  
 * This class uses an underlying fixed thread pool to schedule the submitted tasks. Once a task is
 * submitted, the desired end time for the task is recorded and used to determine the timeout for
 * the task's associated {@link Future}.
 *  
 * The timeout will not be exact as the start time is recorded prior to submitting the
 * {@link Callable}. This may result in an effective timeout that is slightly smaller than expected.
 * The timeout used during initialization should be adjusted accordingly.
 *  
 * The {@link TimeoutTaskExecutor} itself is not a thread-safe class. Only a single thread should
 * submit tasks and complete them. The callback methods will be invoked from the same thread that
 * called TimeoutTaskExecutor.complete(), so the callback methods need not be thread-safe.
 *
 * @param <T>
 *          The return type for the corresponding Callable.
 * @param <C>
 *          The type of Callable submitted to this executor.
 */
@NotThreadSafe
public class TimeoutTaskExecutor<T,C extends Callable<T>> implements AutoCloseable {

  private final static Logger log = LoggerFactory.getLogger(TimeoutTaskExecutor.class);

  private final long timeoutInNanos;
  private final ExecutorService executorService;
  private final BlockingQueue<WrappedTask> startedTasks;
  private final List<WrappedTask> wrappedTasks;

  private SuccessCallback<T,C> successCallback;
  private ExceptionCallback<C> exceptionCallback;
  private TimeoutCallback<C> timeoutCallback;

  private volatile boolean isCompleting = false;

  /**
   * Constructs a new TimeoutTaskExecutor that will use the given number of worker threads and
   * timeout. Takes an expected number of Callables to initialize the underlying data structures
   * appropriately.
   *  
   * If the expectedNumCallables is sized too small, this executor will block on calls to submit()
   * once the internal queue is full.
   *
   * @param numThreads
   *          The number of threads to use.
   * @param timeoutInMillis
   *          The timeout for each task in milliseconds.
   * @param expectedNumCallables
   *          The expected number of callables you will schedule. Note this is used for an
   *          underlying BlockingQueue. If sized too small this will cause blocking when calling
   *          submit().
   * @throws IllegalArgumentException
   *           If numThreads is less than 1 or expectedNumCallables is negative.
   */
  public TimeoutTaskExecutor(int numThreads, long timeoutInMillis, int expectedNumCallables) {
    Preconditions.checkArgument(numThreads >= 1, "Number of threads must be at least 1.");
    Preconditions.checkArgument(expectedNumCallables >= 0,
        "The expected number of callables must be non-negative.");

    this.executorService = Executors.newFixedThreadPool(numThreads);
    this.startedTasks = new ArrayBlockingQueue<>(expectedNumCallables);
    this.timeoutInNanos = TimeUnit.MILLISECONDS.toNanos(timeoutInMillis);
    this.wrappedTasks = new ArrayList<>(expectedNumCallables);
  }

  /**
   * Submits a new task to the executor.
   *
   * @param callable
   *          Task to run
   * @throws ConcurrentModificationException
   *           if this method is invoked while a complete() operation is in progress.
   */
  public void submit(C callable) {
    if (isCompleting) {
      throw new ConcurrentModificationException(
          "TimeoutTaskExecutor is not a thread-safe class but complete() is currently in progress.");
    }

    WrappedTask wt = new WrappedTask(callable);
    wt.future = executorService.submit(wt);
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
    this.successCallback = Objects.requireNonNull(successCallback,
        "Must provide a non-null successCallback.");
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
    this.exceptionCallback = Objects.requireNonNull(exceptionCallback,
        "Must provide a non-null exceptionCallback.");
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
    this.timeoutCallback = Objects.requireNonNull(timeoutCallback,
        "Must provide a non-null timeoutCallback.");
  }

  /**
   * Completes all the current tasks by dispatching to the appropriate callback.
   *
   * @throws IllegalStateException
   *           If all of the callbacks were not registered before calling this method.
   * @throws InterruptedException
   *           If interrupted while awaiting callable results.
   */
  public void complete() throws InterruptedException {
    isCompleting = true;

    try {
      Preconditions.checkState(successCallback != null,
          "Must set a success callback before completing " + this);
      Preconditions.checkState(exceptionCallback != null,
          "Must set an exception callback before completing " + this);
      Preconditions.checkState(timeoutCallback != null,
          "Must set a timeout callback before completing " + this);

      int unfinished = wrappedTasks.size();

      while (unfinished > 0) {
        // poll for twice the timeout which should definitely yield a new running task
        WrappedTask wt = startedTasks.poll(timeoutInNanos * 2, TimeUnit.NANOSECONDS);
        if (wt != null) {
          completeTask(wt);
          wt.hasCompleted = true;
          unfinished--;
        }
      }

      wrappedTasks.clear();
    } finally {
      isCompleting = false;
    }
  }

  private void completeTask(WrappedTask wt) throws InterruptedException {
    try {
      handleSuccess(wt);
    } catch (InterruptedException e) {
      throw e;
    } catch (TimeoutException e) {
      handleTimeout(wt);
    } catch (Exception e) {
      handleException(wt, e);
    }
  }

  private void handleSuccess(WrappedTask wt)
      throws InterruptedException, ExecutionException, TimeoutException {
    long waitTime = wt.endTime - System.nanoTime();
    waitTime = (waitTime < 0 ? 0 : waitTime);
    successCallback.accept(wt.callable, wt.future.get(waitTime, TimeUnit.NANOSECONDS));
  }

  private void handleTimeout(WrappedTask wt) {
    wt.future.cancel(true);
    timeoutCallback.accept(wt.callable);
  }

  private void handleException(WrappedTask wt, Exception e) {
    exceptionCallback.accept(wt.callable, e);
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
   * A wrapper for a Callable that keeps additional information. This tracks the desired end time,
   * if it has completed (either finished or cancelled), and keeps the associated future.
   *
   * The only state shared between the executor thread and the worker threads is the startedTasks
   * BlockingQueue and the endTime variable. The endTime will be set when the worker starts by the
   * worker thread and then read by the executor thread.
   */
  private class WrappedTask implements Callable<T> {
    final C callable;

    // Set by worker thread and read by master thread
    volatile long endTime;

    // Set and read only by master thread
    boolean hasCompleted = false;

    Future<T> future;

    WrappedTask(C callable) {
      this.callable = callable;
    }

    @Override
    public T call() throws Exception {
      endTime = timeoutInNanos + System.nanoTime();
      startedTasks.put(this);
      return callable.call();
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
