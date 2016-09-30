/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate;

import java.util.concurrent.ThreadPoolExecutor;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class FateThreadsTest {

  public ThreadPoolExecutor pool;
  public Fate<Object> fate;
  public AgeOffStore<Object> store;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    pool = EasyMock.createMock(ThreadPoolExecutor.class);
    store = (AgeOffStore<Object>) EasyMock.createMock(AgeOffStore.class);
    fate = new Fate<Object>(new Object(), store);
  }

  private void growTest(int active, int desired) {
    // active threads running
    EasyMock.expect(pool.getActiveCount()).andReturn(active).once();

    // Grow to desired, start (desired-starting) runnables
    pool.setCorePoolSize(desired);
    EasyMock.expectLastCall().once();
    pool.setMaximumPoolSize(desired);
    EasyMock.expectLastCall().once();
    pool.execute(EasyMock.anyObject(Runnable.class));
    EasyMock.expectLastCall().times(desired - active);

    // Grow from active to desired
    runTest(desired);
  }

  private void runTest(int desiredRunners) {
    EasyMock.replay(pool, store);
    fate._ensureThreadsRunning(pool, desiredRunners);
    EasyMock.verify(pool, store);
  }

  @Test
  public void testNonPositiveValues() {
    runTest(0);

    EasyMock.reset(pool, store);

    runTest(-5);
  }

  @Test
  public void testGrowPool() {
    growTest(4, 5);
    EasyMock.reset(pool, store);
    growTest(4, 8);
  }

  @Test
  public void testAttemptOnShrink() {
    // 4 threads running
    EasyMock.expect(pool.getActiveCount()).andReturn(4).once();

    // Shrinking not implementing, nothing should happen.
    runTest(2);
  }

}
