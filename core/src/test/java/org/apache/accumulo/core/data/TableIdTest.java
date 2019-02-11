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
package org.apache.accumulo.core.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests the Table ID class, mainly the internal cache.
 */
public class TableIdTest {
  @Rule
  public TestName name = new TestName();

  @Test
  public void testCacheIncreases() {
    Long initialSize = TableId.cache.asMap().entrySet().stream().count();
    String tableString = "table-" + name.getMethodName();
    TableId table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, TableId.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonical());
  }

  @Test
  public void testCacheNoDuplicates() {
    // the next two lines just preloads the built-ins, since they now exist in a separate class from
    // TableId, and aren't preloaded when the TableId class is referenced
    assertNotSame(RootTable.ID, MetadataTable.ID);
    assertNotSame(RootTable.ID, ReplicationTable.ID);

    String tableString = "table-" + name.getMethodName();
    Long initialSize = TableId.cache.asMap().entrySet().stream().count();
    TableId table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, TableId.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonical());

    // ensure duplicates are not created
    TableId builtInTableId = TableId.of("!0");
    assertSame(MetadataTable.ID, builtInTableId);
    builtInTableId = TableId.of("+r");
    assertSame(RootTable.ID, builtInTableId);
    builtInTableId = TableId.of("+rep");
    assertSame(ReplicationTable.ID, builtInTableId);
    table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, TableId.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonical());
    TableId table2 = TableId.of(tableString);
    assertEquals(initialSize + 1, TableId.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table2.canonical());
    assertSame(table1, table2);
  }

  @Test(timeout = 60_000)
  public void testCacheDecreasesAfterGC() {
    Long initialSize = TableId.cache.asMap().entrySet().stream().count();
    generateJunkCacheEntries();
    Long postGCSize;
    do {
      System.gc();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
      }
      postGCSize = TableId.cache.asMap().entrySet().stream().count();
    } while (postGCSize > initialSize);

    assertTrue("Cache did not decrease with GC.",
        TableId.cache.asMap().entrySet().stream().count() < initialSize);
  }

  private void generateJunkCacheEntries() {
    for (int i = 0; i < 1000; i++)
      TableId.of(new String("table" + i));
  }

}
