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
package org.apache.accumulo.core.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the Table ID class, mainly the internal WeakHashMap.
 */
public class TableTest {

  @After
  public void cleanup() {
    garbageCollect(Table.ID.tableIds.size());
  }

  @Test
  public void testWeakHashMapIncreases() {
    // ensure initial size contains just the built in Table IDs (metadata, root, replication)
    assertEquals(3, Table.ID.tableIds.size());

    String tableString = new String("table-testWeakHashMapIncreases");
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(4, Table.ID.tableIds.size());
    assertEquals(tableString, table1.canonicalID());
  }

  @Test
  public void testWeakHashMapNoDuplicates() {
    String tableString = new String("table-testWeakHashMapNoDuplicates");
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(4, Table.ID.tableIds.size());
    assertEquals(tableString, table1.canonicalID());

    // ensure duplicates are not created
    Table.ID builtInTableId = Table.ID.of("!0");
    assertEquals(Table.ID.METADATA, builtInTableId);
    builtInTableId = Table.ID.of("+r");
    assertEquals(Table.ID.ROOT, builtInTableId);
    builtInTableId = Table.ID.of("+rep");
    assertEquals(Table.ID.REPLICATION, builtInTableId);
    table1 = Table.ID.of(tableString);
    assertEquals(4, Table.ID.tableIds.size());
    assertEquals(tableString, table1.canonicalID());
  }

  @Test
  public void testWeakHashMapDecreases() {
    // get bunch of table IDs that are not used
    for (int i = 0; i < 1000; i++)
      Table.ID.of(new String("table" + i));
  }

  private void garbageCollect(Integer initialSize) {
    System.out.println("GC with initial Size = " + initialSize);
    int count = 0;
    while (Table.ID.tableIds.size() >= initialSize && count < 10) {
      System.gc();
      count++;
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
        break;
      }
      System.out.println("after GC: Size of tableIds = " + Table.ID.tableIds.size());
    }
    assertTrue("Size of WeakHashMap did not decrease after GC.", Table.ID.tableIds.size() < initialSize);
  }

}
