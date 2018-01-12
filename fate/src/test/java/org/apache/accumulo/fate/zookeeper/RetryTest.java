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
package org.apache.accumulo.fate.zookeeper;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class RetryTest {

  private Retry retry;
  long initialWait = 1000l, waitIncrement = 1000l, maxRetries = 5;
  private Retry unlimitedRetry1;
  private Retry unlimitedRetry2;

  @Before
  public void setup() {
    retry = new Retry(maxRetries, initialWait, waitIncrement, maxRetries * 1000l);
    unlimitedRetry1 = new Retry(initialWait, waitIncrement, maxRetries * 1000l);
    unlimitedRetry2 = new Retry(-10, initialWait, waitIncrement, maxRetries * 1000l);
  }

  @Test
  public void canRetryDoesntAlterState() {
    for (int i = 0; i < maxRetries + 1; i++) {
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
    for (int i = 0; i < maxRetries; i++) {
      Assert.assertEquals(i, retry.retriesCompleted());
      // canRetry doesn't alter retry's state
      retry.canRetry();
      Assert.assertEquals(i, retry.retriesCompleted());
      // Using the retry will increase the internal count
      retry.useRetry();
      Assert.assertEquals(i + 1, retry.retriesCompleted());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void usingNonExistentRetryFails() {
    for (int i = 0; i < maxRetries; i++) {
      Assert.assertTrue(retry.canRetry());
      retry.useRetry();
    }
    Assert.assertFalse(retry.canRetry());
    // Calling useRetry when canRetry returns false throws an exception
    retry.useRetry();
  }

  @Test
  public void testWaitIncrement() throws InterruptedException {
    retry = EasyMock.createMockBuilder(Retry.class).addMockedMethod("sleep").createStrictMock();
    retry.setMaxRetries(maxRetries);
    retry.setStartWait(initialWait);
    retry.setWaitIncrement(waitIncrement);
    retry.setMaxWait(maxRetries * 1000l);

    long currentWait = initialWait;
    for (int i = 1; i <= maxRetries; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      currentWait += waitIncrement;
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
    retry.setMaxRetries(maxRetries);
    retry.setStartWait(initialWait);
    retry.setWaitIncrement(waitIncrement);
    // Make the last retry not increment in length
    retry.setMaxWait((maxRetries - 1) * 1000l);

    long currentWait = initialWait;
    for (int i = 1; i <= maxRetries; i++) {
      retry.sleep(currentWait);
      EasyMock.expectLastCall();
      if (i < maxRetries - 1) {
        currentWait += waitIncrement;
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
    Assert.assertFalse(retry.isMaxRetryDisabled());
    Assert.assertTrue(unlimitedRetry1.isMaxRetryDisabled());
    Assert.assertTrue(unlimitedRetry2.isMaxRetryDisabled());
    Assert.assertEquals(Retry.MAX_RETRY_DISABLED, unlimitedRetry1.getMaxRetries());
    Assert.assertEquals(-10, unlimitedRetry2.getMaxRetries());
  }

  @Test
  public void testUnlimitedRetry() {
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      Assert.assertTrue(unlimitedRetry1.canRetry());
      unlimitedRetry1.useRetry();
      Assert.assertTrue(unlimitedRetry2.canRetry());
      unlimitedRetry2.useRetry();
    }
  }
}
