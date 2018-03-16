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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.accumulo.master.TimeoutTaskExecutor.SuccessCallback;
import org.apache.accumulo.master.TimeoutTaskExecutor.TimeoutCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class TimeoutTaskExecutorTest {

  private TimeoutTaskExecutor<String,DummyTask> executor;
  private long timeout = 100;

  private Collection<String> results;
  private Collection<DummyTask> timeouts;

  @Before
  public void setup() {
    int numThreads = 2;
    executor = new TimeoutTaskExecutor<>(Executors.newFixedThreadPool(numThreads), timeout, 3);

    results = new ArrayList<>();
    timeouts = new ArrayList<>();

    executor.onSuccess(new SuccessCallback<String,DummyTask>() {
      @Override
      public void accept(DummyTask task, String result) {
        results.add(result);
      }
    });

    executor.onTimeout(new TimeoutCallback<DummyTask>() {
      @Override
      public void accept(DummyTask task) {
        timeouts.add(task);
      }
    });

    executor.onException(new TimeoutTaskExecutor.ExceptionCallback<DummyTask>() {
      @Override
      public void accept(DummyTask task, Exception e) {
        e.printStackTrace();
        fail("Unexpected exception");
      }
    });
  }

  @Test
  public void shouldExecuteTasks() throws InterruptedException {
    executor.submit(new DummyTask("one", 0));
    executor.submit(new DummyTask("two", 0));

    executor.complete();

    assertThat(results.contains("one"), is(true));
    assertThat(results.contains("two"), is(true));
    assertThat(timeouts.isEmpty(), is(true));
  }

  @Test
  public void shouldReportTimedOutTasks() throws InterruptedException {
    executor.submit(new DummyTask("successful", 0));
    executor.submit(new DummyTask("timeout", timeout * 2));

    executor.complete();

    DummyTask task = Iterables.get(timeouts, 0);

    assertThat(timeouts.size(), is(1));
    assertThat(task.result, is("timeout"));
  }

  @Test
  public void slowTasksShouldNotPreventOthersFromRunning() throws Exception {
    // Clog up the threadpool with slow running tasks
    executor.submit(new DummyTask("slow task 1", Long.MAX_VALUE));
    executor.submit(new DummyTask("slow task 2", Long.MAX_VALUE));
    executor.submit(new DummyTask("slow task 3", Long.MAX_VALUE));
    executor.submit(new DummyTask("good task", 0L));

    executor.complete();

    assertThat(results.size(), is(1));
    assertThat(Iterables.getFirst(results, null), is("good task"));
  }

  @After
  public void tearDown() {
    executor.close();
  }

  private class DummyTask implements Callable<String> {
    private final String result;
    private final long timeout;

    public DummyTask(String result, long timeout) {
      this.result = result;
      this.timeout = timeout;
    }

    @Override
    public String call() throws Exception {
      Thread.sleep(timeout);
      return result;
    }
  }
}
