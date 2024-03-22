/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.accumulo.core.util.Retry.NeedsLogInterval;
import org.apache.accumulo.core.util.Retry.NeedsMaxWait;
import org.apache.accumulo.core.util.Retry.NeedsRetries;
import org.apache.accumulo.core.util.Retry.NeedsRetryDelay;
import org.apache.accumulo.core.util.Retry.NeedsTimeIncrement;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryTest {

  private Retry retry;
  private static final Duration INITIAL_WAIT = Duration.ofSeconds(1);
  private static final Duration WAIT_INC = Duration.ofSeconds(1);
  private static final double BACKOFF_FACTOR = 1.0;
  private static final long MAX_RETRIES = 5;
  private static final Duration LOG_INTERVAL = Duration.ofSeconds(1);
  private Retry unlimitedRetry;

  private static final Logger log = LoggerFactory.getLogger(RetryTest.class);

  @BeforeEach
  public void setup() {
    retry = Retry.builder().maxRetries(MAX_RETRIES).retryAfter(INITIAL_WAIT).incrementBy(WAIT_INC)
        .maxWait(WAIT_INC.multipliedBy(MAX_RETRIES)).backOffFactor(BACKOFF_FACTOR)
        .logInterval(LOG_INTERVAL).createRetry();
    unlimitedRetry = Retry.builder().infiniteRetries().retryAfter(INITIAL_WAIT)
        .incrementBy(WAIT_INC).maxWait(WAIT_INC.multipliedBy(MAX_RETRIES))
        .backOffFactor(BACKOFF_FACTOR).logInterval(LOG_INTERVAL).createRetry();
  }

  @Test
  public void canRetryDoesntAlterState() {
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      assertTrue(retry.canRetry());
    }
  }

  @Test
  public void hasRetriedAfterUse() {
    assertFalse(retry.hasRetried());
    retry.useRetry();
    assertTrue(retry.hasRetried());
  }

  @Test
  public void retriesAreCompleted() {
    for (int i = 0; i < MAX_RETRIES; i++) {
      assertEquals(i, retry.retriesCompleted());
      // canRetry doesn't alter retry's state
      assertTrue(retry.canRetry());
      assertEquals(i, retry.retriesCompleted());
      // Using the retry will increase the internal count
      retry.useRetry();
      assertEquals(i + 1, retry.retriesCompleted());
    }
  }

  @Test
  public void usingNonExistentRetryFails() {
    for (int i = 0; i < MAX_RETRIES; i++) {
      assertTrue(retry.canRetry());
      retry.useRetry();
    }
    assertFalse(retry.canRetry());
    assertThrows(IllegalStateException.class, () -> retry.useRetry(),
        "Calling useRetry when canRetry returns false throws an exception");
  }

  @Test
  public void testWaitIncrement() throws InterruptedException {
    retry = EasyMock.createMockBuilder(Retry.class).addMockedMethod("sleep").createStrictMock();
    retry.setMaxRetries(MAX_RETRIES);
    retry.setStartWait(INITIAL_WAIT);
    retry.setWaitIncrement(WAIT_INC);
    retry.setMaxWait(Duration.ofSeconds(1).multipliedBy(MAX_RETRIES));
    retry.setBackOffFactor(1);
    retry.setDoTimeJitter(false);

    Duration currentWait = INITIAL_WAIT;
    for (int i = 1; i <= MAX_RETRIES; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      currentWait = currentWait.plus(WAIT_INC);
    }

    EasyMock.replay(retry);

    while (retry.canRetry()) {
      retry.useRetry();
      retry.waitForNextAttempt(log, "test wait increment");
    }

    EasyMock.verify(retry);
  }

  @Test
  public void testBackOffFactor() throws InterruptedException {
    retry = EasyMock.createMockBuilder(Retry.class).addMockedMethod("sleep").createStrictMock();
    retry.setMaxRetries(MAX_RETRIES);
    retry.setBackOffFactor(1.5);
    retry.setStartWait(INITIAL_WAIT);
    Duration waitIncrement, currentWait = INITIAL_WAIT;
    retry.setWaitIncrement(WAIT_INC);
    retry.setMaxWait(Duration.ofSeconds(128).multipliedBy(MAX_RETRIES));
    retry.setDoTimeJitter(false);
    double backOfFactor = 1.5, originalBackoff = 1.5;

    for (int i = 1; i <= MAX_RETRIES; i++) {
      retry.sleep(currentWait);
      double waitFactor = backOfFactor;
      backOfFactor *= originalBackoff;
      waitIncrement = Duration.ofMillis((long) (Math.ceil(waitFactor * WAIT_INC.toMillis())));
      Duration tempWait = INITIAL_WAIT.plus(waitIncrement);
      if (tempWait.compareTo(retry.getMaxWait()) > 0) {
        currentWait = retry.getMaxWait();
      } else {
        currentWait = tempWait;
      }
      EasyMock.expectLastCall();
    }

    EasyMock.replay(retry);

    while (retry.canRetry()) {
      retry.useRetry();
      retry.waitForNextAttempt(log, "test backoff factor");
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
    retry.setMaxWait(Duration.ofSeconds(MAX_RETRIES - 1));
    retry.setBackOffFactor(1);
    retry.setDoTimeJitter(false);

    Duration currentWait = INITIAL_WAIT;
    for (int i = 1; i <= MAX_RETRIES; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      if (i < MAX_RETRIES - 1) {
        currentWait = currentWait.plus(WAIT_INC);
      }
    }

    EasyMock.replay(retry);

    while (retry.canRetry()) {
      retry.useRetry();
      retry.waitForNextAttempt(log, "test bounded wait increment");
    }

    EasyMock.verify(retry);
  }

  @Test
  public void testIsMaxRetryDisabled() {
    assertFalse(retry.hasInfiniteRetries());
    assertTrue(unlimitedRetry.hasInfiniteRetries());
    assertEquals(-1, unlimitedRetry.getMaxRetries());
  }

  @Test
  public void testUnlimitedRetry() {
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      assertTrue(unlimitedRetry.canRetry());
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
    assertTrue(i > 10);

  }

  @Test
  public void testMaxRetries() {
    NeedsRetries builder = Retry.builder();
    builder.maxRetries(10);
    builder.maxRetries(0);
    assertThrows(IllegalArgumentException.class, () -> builder.maxRetries(-1),
        "Should not allow negative retries");
  }

  @Test
  public void testInitialWait() {
    NeedsRetryDelay builder = Retry.builder().maxRetries(10);
    builder.retryAfter(Duration.ofNanos(10));
    builder.retryAfter(Duration.ofMillis(10));
    builder.retryAfter(Duration.ofDays(10));
    builder.retryAfter(Duration.ofNanos(0));
    builder.retryAfter(Duration.ofMillis(0));
    builder.retryAfter(Duration.ofDays(0));

    assertThrows(IllegalArgumentException.class, () -> builder.retryAfter(Duration.ofNanos(-1)),
        "Should not allow negative wait times");
  }

  @Test
  public void testIncrementBy() {
    NeedsTimeIncrement builder = Retry.builder().maxRetries(10).retryAfter(Duration.ofMillis(10));
    builder.incrementBy(Duration.ofDays(10));
    builder.incrementBy(Duration.ofHours(10));
    builder.incrementBy(Duration.ofNanos(10));
    builder.incrementBy(Duration.ofDays(0));
    builder.incrementBy(Duration.ofHours(0));
    builder.incrementBy(Duration.ofNanos(0));

    assertThrows(IllegalArgumentException.class, () -> builder.incrementBy(Duration.ofNanos(-1)),
        "Should not allow negative increments");
  }

  @Test
  public void testMaxWait() {
    NeedsMaxWait builder = Retry.builder().maxRetries(10).retryAfter(Duration.ofMillis(15))
        .incrementBy(Duration.ofMillis(10));
    builder.maxWait(Duration.ofMillis(15));
    builder.maxWait(Duration.ofMillis(16));

    assertThrows(IllegalArgumentException.class, () -> builder.maxWait(Duration.ofMillis(14)),
        "Max wait time should be greater than or equal to initial wait time");
  }

  @Test
  public void testLogInterval() {
    NeedsLogInterval builder = Retry.builder().maxRetries(10).retryAfter(Duration.ofMillis(15))
        .incrementBy(Duration.ofMillis(10)).maxWait(Duration.ofMinutes(16)).backOffFactor(1);
    builder.logInterval(Duration.ofDays(10));
    builder.logInterval(Duration.ofHours(10));
    builder.logInterval(Duration.ofNanos(10));
    builder.logInterval(Duration.ofDays(0));
    builder.logInterval(Duration.ofHours(0));
    builder.logInterval(Duration.ofNanos(0));

    assertThrows(IllegalArgumentException.class, () -> builder.logInterval(Duration.ofNanos(-1)),
        "Log interval must not be negative");
  }

  @Test
  public void properArgumentsInRetry() {
    long maxRetries = 10;
    Duration startWait = Duration.ofMillis(50);
    Duration maxWait = Duration.ofMillis(5000);
    Duration waitIncrement = Duration.ofMillis(500);
    Duration logInterval = Duration.ofMillis(10000);

    RetryFactory factory =
        Retry.builder().maxRetries(maxRetries).retryAfter(startWait).incrementBy(waitIncrement)
            .maxWait(maxWait).backOffFactor(1).logInterval(logInterval).createFactory();
    Retry retry = factory.createRetry();

    assertEquals(maxRetries, retry.getMaxRetries());
    assertEquals(startWait.toMillis(), retry.getCurrentWait().toMillis());
    assertEquals(maxWait.toMillis(), retry.getMaxWait().toMillis());
    assertEquals(waitIncrement.toMillis(), retry.getWaitIncrement().toMillis());
    assertEquals(logInterval.toMillis(), retry.getLogInterval().toMillis());
  }

  @Test
  public void properArgumentsInUnlimitedRetry() {
    Duration startWait = Duration.ofMillis(50);
    Duration maxWait = Duration.ofSeconds(5);
    Duration waitIncrement = Duration.ofMillis(500);
    Duration logInterval = Duration.ofSeconds(10);
    double waitFactor = 1.0;

    RetryFactory factory =
        Retry.builder().infiniteRetries().retryAfter(startWait).incrementBy(waitIncrement)
            .maxWait(maxWait).backOffFactor(waitFactor).logInterval(logInterval).createFactory();
    Retry retry = factory.createRetry();

    assertEquals(-1, retry.getMaxRetries());
    assertEquals(startWait.toMillis(), retry.getCurrentWait().toMillis());
    assertEquals(maxWait.toMillis(), retry.getMaxWait().toMillis());
    assertEquals(waitIncrement.toMillis(), retry.getWaitIncrement().toMillis());
    assertEquals(logInterval.toMillis(), retry.getLogInterval().toMillis());
  }

  @Test
  public void testInfiniteRetryWithBackoff() throws InterruptedException {
    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofMillis(500)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();
    for (int i = 0; i < 100; i++) {
      try {
        retry.waitForNextAttempt(log, i + "");
      } catch (IllegalArgumentException e) {
        log.error("Failed on iteration: {}", i);
        throw e;
      }
    }
  }

  @Nested
  public class MaxRetriesWithinDuration {

    @Test
    public void noIncrement() {
      Duration retriesForDuration = Duration.ofSeconds(3);
      Duration retryAfter = Duration.ofMillis(100);
      Retry retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration)
          .retryAfter(retryAfter).incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(1000))
          .backOffFactor(1.0).logInterval(Duration.ofMinutes(3)).createRetry();

      // with no increment, the number of retries should be the duration divided by the retryAfter
      // (which is used as the initial wait and in this case does not change)
      long expectedRetries = retriesForDuration.dividedBy(retryAfter);
      assertEquals(expectedRetries, retry.getMaxRetries());

      // try again with lots of expected retries
      retriesForDuration = Duration.ofSeconds(30);
      retryAfter = Duration.ofMillis(10);
      retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration).retryAfter(retryAfter)
          .incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(1000)).backOffFactor(1.0)
          .logInterval(Duration.ofMinutes(3)).createRetry();

      expectedRetries = retriesForDuration.dividedBy(retryAfter);
      assertEquals(expectedRetries, retry.getMaxRetries());
    }

    @Test
    public void withIncrement() {
      final Duration retriesForDuration = Duration.ofMillis(1500);
      final Duration retryAfter = Duration.ofMillis(100);
      final Duration increment = Duration.ofMillis(100);

      Retry retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration)
          .retryAfter(retryAfter).incrementBy(increment).maxWait(Duration.ofMillis(1000))
          .backOffFactor(1.0).logInterval(Duration.ofMinutes(3)).createRetry();

      // the max retries should be calculated like this:
      // 1. 100
      // 2. 100 + 100 = 200
      // 3. 200 + 100 = 300
      // 4. 300 + 100 = 400
      // 5. 400 + 100 = 500

      // 100 + 200 + 300 + 400 + 500 = 1500

      assertEquals(5, retry.getMaxRetries());
    }

    @Test
    public void withBackoffFactorAndMaxWait() {
      final Duration retriesForDuration = Duration.ofSeconds(4);
      final Duration retryAfter = Duration.ofMillis(100);
      Retry retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration)
          .retryAfter(retryAfter).incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(500))
          .backOffFactor(1.5).logInterval(Duration.ofMinutes(3)).createRetry();

      // max retries should be calculated like this:
      // 1. 100
      // 2. 100 * 1.5 = 150
      // 3. 150 * 1.5 = 225
      // 4. 225 * 1.5 = 337
      // 5. 337 * 1.5 = 505 (which is greater than the max wait of 500 so its capped)

      // 100 + 150 + 225 + 337 + 500 + 500 + 500 + 500 + 500 + 500 = 3812
      assertEquals(10, retry.getMaxRetries());
    }

    @Test
    public void smallDuration() {
      Duration retriesForDuration = Duration.ofMillis(0);
      final Duration retryAfter = Duration.ofMillis(100);
      Retry retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration)
          .retryAfter(retryAfter).incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(500))
          .backOffFactor(1.5).logInterval(Duration.ofMinutes(3)).createRetry();
      assertEquals(0, retry.getMaxRetries());

      retriesForDuration = Duration.ofMillis(99);
      assertTrue(retriesForDuration.compareTo(retryAfter) < 0);
      retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration).retryAfter(retryAfter)
          .incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(500)).backOffFactor(1.5)
          .logInterval(Duration.ofMinutes(3)).createRetry();
      assertEquals(0, retry.getMaxRetries());
    }

    @Test
    public void equalDurationAndInitialWait() {
      final Duration retriesForDuration = Duration.ofMillis(100);
      final Duration retryAfter = Duration.ofMillis(100);
      assertEquals(0, retriesForDuration.compareTo(retryAfter));
      Retry retry = Retry.builder().maxRetriesWithinDuration(retriesForDuration)
          .retryAfter(retryAfter).incrementBy(Duration.ofMillis(0)).maxWait(Duration.ofMillis(500))
          .backOffFactor(1.5).logInterval(Duration.ofMinutes(3)).createRetry();
      assertEquals(1, retry.getMaxRetries());
    }
  }
}
