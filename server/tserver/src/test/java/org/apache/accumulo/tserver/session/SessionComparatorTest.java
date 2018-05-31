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
package org.apache.accumulo.tserver.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SessionComparatorTest {

  @Test
  public void testSingleScanMultiScanNoRun() {
    long time = System.currentTimeMillis();
    ScanSession sessionA = emptyScanSession();
    sessionA.lastAccessTime = 0;
    sessionA.maxIdleAccessTime = 0;
    sessionA.startTime = time - 1000;

    MultiScanSession sessionB = emptyMultiScanSession();
    sessionB.lastAccessTime = 0;
    sessionB.maxIdleAccessTime = 1000;
    sessionB.startTime = time - 800;

    ScanSession sessionC = emptyScanSession();
    sessionC.lastAccessTime = 0;
    sessionC.maxIdleAccessTime = 1000;
    sessionC.startTime = time - 800;

    // a has never run, so it should be given priority
    SingleRangePriorityComparator comparator = new SingleRangePriorityComparator();
    assertEquals(-1, comparator.compareSession(sessionA, sessionB));

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    assertEquals(1, comparator.compareSession(sessionB, sessionA));

    // now let's assume they have been executed

    assertEquals(1, comparator.compareSession(sessionA, sessionC));

    assertEquals(0, comparator.compareSession(sessionC, sessionC));

  }

  @Test
  public void testSingleScanRun() {
    long time = System.currentTimeMillis();
    ScanSession sessionA = emptyScanSession();
    sessionA.lastAccessTime = 0;
    sessionA.setLastExecutionTime(time);
    sessionA.maxIdleAccessTime = 1000;
    sessionA.startTime = time - 1000;

    ScanSession sessionB = emptyScanSession();
    sessionB.lastAccessTime = 0;
    sessionB.setLastExecutionTime(time - 2000);
    sessionB.maxIdleAccessTime = 1000;
    sessionB.startTime = time - 800;

    // b is newer
    SingleRangePriorityComparator comparator = new SingleRangePriorityComparator();
    assertEquals(1, comparator.compareSession(sessionA, sessionB));

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    assertTrue(comparator.compareSession(sessionB, sessionA) < 0);

    sessionB.setLastExecutionTime(time);
    sessionA.setLastExecutionTime(time - 2000);

    assertTrue(comparator.compareSession(sessionA, sessionB) < 0);

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    int comp = comparator.compareSession(sessionB, sessionA);
    assertTrue("comparison is " + comp, comp >= 1);
  }

  @Test
  public void testSingleScanMultiScanRun() {
    long time = System.currentTimeMillis();
    ScanSession sessionA = emptyScanSession();
    sessionA.lastAccessTime = 0;
    sessionA.setLastExecutionTime(time);
    sessionA.maxIdleAccessTime = 1000;
    sessionA.startTime = time - 1000;

    MultiScanSession sessionB = emptyMultiScanSession();
    sessionB.lastAccessTime = 0;
    sessionB.setLastExecutionTime(time - 2000);
    sessionB.maxIdleAccessTime = 1000;
    sessionB.startTime = time - 800;

    // b is newer
    SingleRangePriorityComparator comparator = new SingleRangePriorityComparator();
    assertEquals(-1, comparator.compareSession(sessionA, sessionB));

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    assertTrue(comparator.compareSession(sessionB, sessionA) > 0);

    sessionB.setLastExecutionTime(time);
    sessionA.setLastExecutionTime(time - 2000);

    assertTrue(comparator.compareSession(sessionA, sessionB) < 0);

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    int comp = comparator.compareSession(sessionB, sessionA);
    assertTrue("comparison is " + comp, comp > 0);
  }

  @Test
  public void testMultiScanRun() {
    long time = System.currentTimeMillis();
    ScanSession sessionA = emptyScanSession();
    sessionA.lastAccessTime = 0;
    sessionA.setLastExecutionTime(time);
    sessionA.maxIdleAccessTime = 1000;
    sessionA.startTime = time - 1000;

    ScanSession sessionB = emptyScanSession();
    sessionB.lastAccessTime = 0;
    sessionB.setLastExecutionTime(time - 2000);
    sessionB.maxIdleAccessTime = 1000;
    sessionB.startTime = time - 800;

    // b is newer
    SingleRangePriorityComparator comparator = new SingleRangePriorityComparator();
    assertEquals(1, comparator.compareSession(sessionA, sessionB));

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    assertTrue(comparator.compareSession(sessionB, sessionA) < 0);

    sessionB.setLastExecutionTime(time);
    sessionA.setLastExecutionTime(time - 2000);

    
    assertTrue(comparator.compareSession(sessionA, sessionB) < 0);

    // b is before a in queue, b has never run, but because a is single
    // we should be given priority
    int comp = comparator.compareSession(sessionB, sessionA);
    assertTrue("comparison is " + comp, comp >= 1);
  }

  private static ScanSession emptyScanSession() {
    return new ScanSession(null, null, null, null, null, null, 0, 0, null);
  }

  private static MultiScanSession emptyMultiScanSession() {
    return new MultiScanSession(null, null, null, null, null, null, null, 0, null);
  }
}
