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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.junit.jupiter.api.Test;

public class ThreadPoolsTest {
  @Test
  public void testExecuteUncaught() throws InterruptedException {

    var tq = new LinkedBlockingQueue<Throwable>();
    Thread.UncaughtExceptionHandler ueh = (thread, throwable) -> {
      tq.add(throwable);
    };
    var threadPools = ThreadPools.getClientThreadPools(DefaultConfiguration.getInstance(), ueh);

    var pool1 = threadPools.getPoolBuilder("test").build();
    var pool2 = threadPools.createScheduledExecutorService(1, "test2");
    int i = 0;
    // Test a normal thread pool and scheduled thread pool
    for (var pool : List.of(pool1, pool2)) {
      assertTrue(tq.isEmpty());

      String msg = "msg" + i;

      // ensure this error makes its way to the uncaught exception handler configured for the pool
      pool.execute(() -> {
        throw new Error(msg);
      });

      var throwable1 = tq.take();
      assertEquals(msg, throwable1.getMessage());
      assertEquals(Error.class, throwable1.getClass());
      i++;
    }
  }
}
