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
package org.apache.accumulo.monitor.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.fail;

import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.monitor.util.AccumuloMonitorAppender.MonitorLocation;
import org.apache.accumulo.monitor.util.AccumuloMonitorAppender.MonitorTracker;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class AccumuloMonitorAppenderTest {

  @Rule
  public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testActivateOptions() {
    try (AccumuloMonitorAppender appender = new AccumuloMonitorAppender()) {
      appender.executorService.shutdown();
      // simulate tracker having already been scheduled
      appender.trackerScheduled.compareAndSet(false, true);
      appender.activateOptions();
      // activateOptions should not trigger a RejectedExecutionException, because we tricked it into thinking it was already called, and therefore it did not
      // schedule the tracker after shutting down
    }

    exception.expect(RejectedExecutionException.class);
    try (AccumuloMonitorAppender appender = new AccumuloMonitorAppender()) {
      appender.executorService.shutdown();
      appender.activateOptions();
      fail("Calling activateOptions should have triggered a RejectedExecutionException");
      // this ensures that the activateOptions correctly attempts to schedule a worker
    }
  }

  @Test
  public void testExecutorService() throws InterruptedException, ExecutionException {
    ScheduledExecutorService executorService = null;
    AtomicLong counter = new AtomicLong(2);
    try (AccumuloMonitorAppender appender = new AccumuloMonitorAppender()) {
      executorService = appender.executorService;

      // make sure executor service is started and running
      Assert.assertEquals(false, executorService.isShutdown());
      Assert.assertEquals(false, executorService.isTerminated());

      // make sure executor service executes tasks
      ScheduledFuture<Long> future = executorService.schedule(() -> counter.getAndIncrement(), 1, TimeUnit.MILLISECONDS);
      Assert.assertEquals(Long.valueOf(2), future.get());
      Assert.assertEquals(3, counter.get());

      // schedule a task that won't finish
      executorService.schedule(() -> counter.getAndIncrement(), 1, TimeUnit.DAYS);

      // make sure executor service is still running
      Assert.assertEquals(false, executorService.isShutdown());
      Assert.assertEquals(false, executorService.isTerminated());
    }
    // verify that closing the appender shuts down the executor service threads
    Assert.assertEquals(true, executorService.isShutdown());
    executorService.awaitTermination(5, TimeUnit.SECONDS);
    Assert.assertEquals(true, executorService.isTerminated());

    // verify executor service did not wait for scheduled task to run
    Assert.assertEquals(3, counter.get());
  }

  @Test
  public void testMonitorTracker() throws InterruptedException {
    AtomicLong currentLoc = new AtomicLong(0);
    Supplier<MonitorLocation> locSupplier = () -> {
      long loc = currentLoc.get();
      // for simplicity, create the location name from a number (0 represents no location)
      byte[] location = loc == 0 ? null : ("loc" + loc).getBytes(UTF_8);
      return new MonitorLocation(loc, location);
    };
    Function<MonitorLocation,AppenderSkeleton> appenderFactory = newLocation -> new AppenderSkeleton() {

      {
        this.name = "Appender for " + newLocation.getLocation();
      }

      @Override
      public boolean requiresLayout() {
        return false;
      }

      @Override
      public void close() {}

      @Override
      protected void append(LoggingEvent event) {}

    };

    try (AccumuloMonitorAppender parent = new AccumuloMonitorAppender()) {
      parent.setFrequency(1); // make it check frequently (every 1 millisecond)
      parent.setTracker(new MonitorTracker(parent, locSupplier, appenderFactory));
      parent.activateOptions();

      // initially there are no appenders
      Assert.assertTrue(parent.getAllAppenders() == null);
      updateLocAndVerify(currentLoc, parent, 0);
      updateLocAndVerify(currentLoc, parent, 10);

      // verify it's the same after a few times
      // this verifies the logic in the tracker's run method which compares current location with last to see if a change occurred
      AppenderSkeleton lastAppender = (AppenderSkeleton) parent.getAllAppenders().nextElement();
      for (int x = 0; x < 10; x++) {
        Thread.sleep(10);
        AppenderSkeleton currentAppender = (AppenderSkeleton) parent.getAllAppenders().nextElement();
        Assert.assertSame(lastAppender, currentAppender);
      }

      updateLocAndVerify(currentLoc, parent, 3);
      updateLocAndVerify(currentLoc, parent, 0);
      updateLocAndVerify(currentLoc, parent, 0);
      updateLocAndVerify(currentLoc, parent, 12);
      updateLocAndVerify(currentLoc, parent, 0);
      updateLocAndVerify(currentLoc, parent, 335);

      updateLocAndVerify(currentLoc, parent, 0);
      // verify we removed all the appenders
      Assert.assertFalse(parent.getAllAppenders().hasMoreElements());
    }
  }

  private static void updateLocAndVerify(AtomicLong currentLoc, AccumuloMonitorAppender parent, int newLoc) {
    // set the new location
    currentLoc.set(newLoc);
    // wait for the appender to notice the change
    while (!verifyAppender(parent, newLoc == 0 ? null : ("loc" + newLoc))) {}
  }

  private static boolean verifyAppender(AccumuloMonitorAppender parent, String newLocName) {
    Enumeration<?> childAppenders = parent.getAllAppenders();
    if (newLocName == null) {
      return childAppenders == null || !childAppenders.hasMoreElements();
    }
    if (childAppenders == null || !childAppenders.hasMoreElements()) {
      return false;
    }
    AppenderSkeleton child = (AppenderSkeleton) childAppenders.nextElement();
    if (childAppenders.hasMoreElements()) {
      Assert.fail("Appender should never have more than one child");
    }
    return ("Appender for " + newLocName).equals(child.getName());
  }

}
