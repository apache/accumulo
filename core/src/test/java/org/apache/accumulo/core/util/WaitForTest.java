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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitForTest {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForTest.class);

  private static class TimedCondition implements WaitFor.Condition {
    private final long startNanos = System.nanoTime();

    @Override
    public boolean isSatisfied() throws Exception {
      return (System.nanoTime() - startNanos) > MILLISECONDS.toNanos(1_100);
    }
  }

  @Test
  public void goPathTest() {

    TimedCondition c1 = new TimedCondition();

    long start = System.nanoTime();

    WaitFor.builder(c1).upTo(5, SECONDS).withDelay(250, MILLISECONDS).waitFor();

    LOG.info("Condition passed in {} mills", NANOSECONDS.toMillis(System.nanoTime() - start));

    assertTrue(System.nanoTime() - start < SECONDS.toNanos(5));

  }

  @Test
  public void pauseTest() {
    long start = System.nanoTime();
    assertThrows(IllegalStateException.class,
        () -> WaitFor.builder(() -> false).upTo(1, SECONDS).withDelay(250, MILLISECONDS)
            .withProgressMsg("a message").withFailMsg("forced fail").waitFor());

    assertTrue(System.nanoTime() - start < SECONDS.toNanos(5));
  }

  @Test
  public void invalidSleepTest() {
    assertThrows(IllegalArgumentException.class,
        () -> WaitFor.builder(() -> false).upTo(0, SECONDS).waitFor());
  }

  @Test
  public void invalidDurationTest() {
    assertThrows(IllegalArgumentException.class,
        () -> WaitFor.builder(() -> false).withDelay(-1, SECONDS).waitFor());
  }
}
