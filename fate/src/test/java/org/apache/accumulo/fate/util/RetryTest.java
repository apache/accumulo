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
package org.apache.accumulo.fate.util;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.util.Retry.NeedsLogInterval;
import org.apache.accumulo.fate.util.Retry.NeedsMaxWait;
import org.apache.accumulo.fate.util.Retry.NeedsRetries;
import org.apache.accumulo.fate.util.Retry.NeedsRetryDelay;
import org.apache.accumulo.fate.util.Retry.NeedsTimeIncrement;
import org.apache.accumulo.fate.util.Retry.RetryFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

public class RetryTest {

  private Retry retry;
  private static final long INITIAL_WAIT = 1000;
  private static final long WAIT_INC = 1000;
  private static final long MAX_RETRIES = 5;
  private static final long LOG_INTERVAL = 1000;
  private Retry unlimitedRetry;
  private static final TimeUnit MS = MILLISECONDS;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    retry = Retry.builder().maxRetries(MAX_RETRIES).retryAfter(INITIAL_WAIT, MS).incrementBy(WAIT_INC, MS).maxWait(MAX_RETRIES * WAIT_INC, MS)
        .logInterval(LOG_INTERVAL, MS).createRetry();
    unlimitedRetry = Retry.builder().infiniteRetries().retryAfter(INITIAL_WAIT, MS).incrementBy(WAIT_INC, MS).maxWait(MAX_RETRIES * WAIT_INC, MS)
        .logInterval(LOG_INTERVAL, MS).createRetry();
  }

  @Test
  public void canRetryDoesntAlterState() {
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      Assert.assertTrue(retry.canRetry());
    }
  }

  @Test
  public void hasRetriedAfterUse() {
    Assert.assertFalse(retry.hasRetried());
    retry.useRetry();
    Assert.assertTrue(retry.hasRetried());
  }

  @Test
  public void retriesAreCompleted() {
    for (int i = 0; i < MAX_RETRIES; i++) {
      Assert.assertEquals(i, retry.retriesCompleted());
      // canRetry doesn't alter retry's state
      retry.canRetry();
      Assert.assertEquals(i, retry.retriesCompleted());
      // Using the retry will increase the internal count
      retry.useRetry();
      Assert.assertEquals(i + 1, retry.retriesCompleted());
    }
  }

  @Test
  public void usingNonExistentRetryFails() {
    for (int i = 0; i < MAX_RETRIES; i++) {
      Assert.assertTrue(retry.canRetry());
      retry.useRetry();
    }
    Assert.assertFalse(retry.canRetry());

    // Calling useRetry when canRetry returns false throws an exception
    exception.expect(IllegalStateException.class);
    retry.useRetry();
    Assert.fail("previous command should have thrown IllegalStateException");
  }

  @Test
  public void testWaitIncrement() throws InterruptedException {
    retry = EasyMock.createMockBuilder(Retry.class).addMockedMethod("sleep").createStrictMock();
    retry.setMaxRetries(MAX_RETRIES);
    retry.setStartWait(INITIAL_WAIT);
    retry.setWaitIncrement(WAIT_INC);
    retry.setMaxWait(MAX_RETRIES * 1000);

    long currentWait = INITIAL_WAIT;
    for (int i = 1; i <= MAX_RETRIES; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      currentWait += WAIT_INC;
    }

    EasyMock.replay(retry);

    while (retry.canRetry()) {
      retry.useRetry();
      retry.waitForNextAttempt();
    }

    EasyMock.verify(retry);
  }

  @Test
  public void testBoundedWaitIncrement() throws InterruptedException {
    retry = EasyMock.createMockBuilder(Retry.class).addMockedMethod("sleep").createStrictMock();
    retry.setMaxRetries(MAX_RETRIES);
    retry.setStartWait(INITIAL_WAIT);
    retry.setWaitIncrement(WAIT_INC);
    // Make the last retry not increment in length
    retry.setMaxWait((MAX_RETRIES - 1) * 1000);

    long currentWait = INITIAL_WAIT;
    for (int i = 1; i <= MAX_RETRIES; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      if (i < MAX_RETRIES - 1) {
        currentWait += WAIT_INC;
      }
    }

    EasyMock.replay(retry);

    while (retry.canRetry()) {
      retry.useRetry();
      retry.waitForNextAttempt();
    }

    EasyMock.verify(retry);
  }

  @Test
  public void testIsMaxRetryDisabled() {
    Assert.assertFalse(retry.hasInfiniteRetries());
    Assert.assertTrue(unlimitedRetry.hasInfiniteRetries());
    Assert.assertEquals(-1, unlimitedRetry.getMaxRetries());
  }

  @Test
  public void testUnlimitedRetry() {
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      Assert.assertTrue(unlimitedRetry.canRetry());
      unlimitedRetry.useRetry();
    }
  }

  @Test
  public void testLogging() {
    Logger testLogger = EasyMock.createMock(Logger.class);
    EasyMock.expect(testLogger.isDebugEnabled()).andReturn(true);
    testLogger.debug(EasyMock.anyObject(String.class));
    EasyMock.expectLastCall().times(1);
    EasyMock.expect(testLogger.isTraceEnabled()).andReturn(true).anyTimes();
    testLogger.trace(EasyMock.anyObject(String.class));
    EasyMock.expectLastCall().anyTimes();
    testLogger.warn(EasyMock.anyObject(String.class));
    EasyMock.expectLastCall().times(3, 5);
    EasyMock.replay(testLogger);

    // we want to do this for 5 second and observe the log messages
    long start = System.currentTimeMillis();
    long end = System.currentTimeMillis();
    int i = 0;
    for (; (end - start < 5000) && (i < Integer.MAX_VALUE); i++) {
      unlimitedRetry.logRetry(testLogger, "failure message");
      unlimitedRetry.useRetry();
      end = System.currentTimeMillis();
    }

    // now observe what log messages we got which should be around 5 +- 1
    EasyMock.verify(testLogger);
    Assert.assertTrue(i > 10);

  }

  @Test
  public void testMaxRetries() {
    NeedsRetries builder = Retry.builder();
    builder.maxRetries(10);
    builder.maxRetries(0);
    exception.expect(IllegalArgumentException.class);
    builder.maxRetries(-1);
    Assert.fail("Should not allow negative retries");
  }

  @Test
  public void testInitialWait() {
    NeedsRetryDelay builder = Retry.builder().maxRetries(10);
    builder.retryAfter(10, NANOSECONDS);
    builder.retryAfter(10, MILLISECONDS);
    builder.retryAfter(10, DAYS);
    builder.retryAfter(0, NANOSECONDS);
    builder.retryAfter(0, MILLISECONDS);
    builder.retryAfter(0, DAYS);

    exception.expect(IllegalArgumentException.class);
    builder.retryAfter(-1, NANOSECONDS);
    Assert.fail("Should not allow negative wait times");
  }

  @Test
  public void testIncrementBy() {
    NeedsTimeIncrement builder = Retry.builder().maxRetries(10).retryAfter(10, MILLISECONDS);
    builder.incrementBy(10, DAYS);
    builder.incrementBy(10, HOURS);
    builder.incrementBy(10, NANOSECONDS);
    builder.incrementBy(0, DAYS);
    builder.incrementBy(0, HOURS);
    builder.incrementBy(0, NANOSECONDS);

    exception.expect(IllegalArgumentException.class);
    builder.incrementBy(-1, NANOSECONDS);
    Assert.fail("Should not allow negative increments");
  }

  @Test
  public void testMaxWait() {
    NeedsMaxWait builder = Retry.builder().maxRetries(10).retryAfter(15, MILLISECONDS).incrementBy(10, MILLISECONDS);
    builder.maxWait(15, MILLISECONDS);
    builder.maxWait(16, MILLISECONDS);

    exception.expect(IllegalArgumentException.class);
    builder.maxWait(14, MILLISECONDS);
    Assert.fail("Max wait time should be greater than or equal to initial wait time");
  }

  @Test
  public void testLogInterval() {
    NeedsLogInterval builder = Retry.builder().maxRetries(10).retryAfter(15, MILLISECONDS).incrementBy(10, MILLISECONDS).maxWait(16, MINUTES);
    builder.logInterval(10, DAYS);
    builder.logInterval(10, HOURS);
    builder.logInterval(10, NANOSECONDS);
    builder.logInterval(0, DAYS);
    builder.logInterval(0, HOURS);
    builder.logInterval(0, NANOSECONDS);

    exception.expect(IllegalArgumentException.class);
    builder.logInterval(-1, NANOSECONDS);
    Assert.fail("Log interval must not be negative");
  }

  @Test
  public void properArgumentsInRetry() {
    long maxRetries = 10, startWait = 50L, maxWait = 5000L, waitIncrement = 500L, logInterval = 10000L;
    RetryFactory factory = Retry.builder().maxRetries(maxRetries).retryAfter(startWait, MS).incrementBy(waitIncrement, MS).maxWait(maxWait, MS)
        .logInterval(logInterval, MS).createFactory();
    Retry retry = factory.createRetry();

    Assert.assertEquals(maxRetries, retry.getMaxRetries());
    Assert.assertEquals(startWait, retry.getCurrentWait());
    Assert.assertEquals(maxWait, retry.getMaxWait());
    Assert.assertEquals(waitIncrement, retry.getWaitIncrement());
    Assert.assertEquals(logInterval, retry.getLogInterval());
  }

  @Test
  public void properArgumentsInUnlimitedRetry() {
    long startWait = 50L, maxWait = 5000L, waitIncrement = 500L, logInterval = 10000L;
    RetryFactory factory = Retry.builder().infiniteRetries().retryAfter(startWait, MS).incrementBy(waitIncrement, MS).maxWait(maxWait, MS)
        .logInterval(logInterval, MS).createFactory();
    Retry retry = factory.createRetry();

    Assert.assertEquals(-1, retry.getMaxRetries());
    Assert.assertEquals(startWait, retry.getCurrentWait());
    Assert.assertEquals(maxWait, retry.getMaxWait());
    Assert.assertEquals(waitIncrement, retry.getWaitIncrement());
    Assert.assertEquals(logInterval, retry.getLogInterval());
  }

}
