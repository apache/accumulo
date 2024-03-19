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
package org.apache.accumulo.core.util.threads;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ThreadPoolExecutorBuilderTest {

  private final ThreadPools serverPool = ThreadPools.getServerThreadPools();

  @Test
  public void builderDefaultsTest() {
    var p = serverPool.getPoolBuilder("defaults").build();
    assertEquals(0, p.getCorePoolSize());
    assertEquals(1, p.getMaximumPoolSize());
    assertEquals(3L, p.getKeepAliveTime(MINUTES));
  }

  @Test
  public void builderInvalidNumCoreTest() {
    assertThrows(IllegalArgumentException.class,
        () -> serverPool.getPoolBuilder("test1").numCoreThreads(-1).build());
  }

  @Test
  public void builderInvalidNumMaxThreadsTest() {
    // max threads must be > core threads
    assertThrows(IllegalArgumentException.class,
        () -> serverPool.getPoolBuilder("test1").numCoreThreads(2).numMaxThreads(1).build());
  }

  @Test
  public void builderPoolCoreMaxTest() {
    var p = serverPool.getPoolBuilder("test1").numCoreThreads(1).numMaxThreads(2).build();
    assertEquals(1, p.getCorePoolSize());
    assertEquals(2, p.getMaximumPoolSize());
  }

  @Test
  public void builderFixedPoolTest() {
    var p = serverPool.getPoolBuilder("test1").numCoreThreads(2).build();
    assertEquals(2, p.getCorePoolSize());
    assertEquals(2, p.getMaximumPoolSize());
  }

  @Test
  public void buildeSetTimeoutTest() {
    var p = serverPool.getPoolBuilder("test1").withTimeOut(0L, MILLISECONDS).build();
    assertEquals(0, p.getCorePoolSize());
    assertEquals(1, p.getMaximumPoolSize());
    assertEquals(0L, p.getKeepAliveTime(MINUTES));

    var p2 = serverPool.getPoolBuilder("test1").withTimeOut(123L, MILLISECONDS).build();
    assertEquals(0, p2.getCorePoolSize());
    assertEquals(1, p2.getMaximumPoolSize());
    assertEquals(123L, p2.getKeepAliveTime(MILLISECONDS));
  }
}
