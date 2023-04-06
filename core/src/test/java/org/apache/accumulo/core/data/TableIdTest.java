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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.accumulo.core.WithTestNames;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests the Table ID class, mainly the internal cache.
 */
public class TableIdTest extends WithTestNames {

  private static final Logger LOG = LoggerFactory.getLogger(TableIdTest.class);

  private static long cacheCount() {
    // guava cache size() is approximate, and can include garbage-collected entries
    // so we iterate to get the actual cache size
    return TableId.cache.asMap().entrySet().stream().count();
  }

  @Test
  public void testCacheNoDuplicates() {

    // the next line just preloads the built-ins, since they now exist in a separate class from
    // TableId, and aren't preloaded when the TableId class is referenced
    assertNotSame(RootTable.ID, MetadataTable.ID);

    String tableString = "table-" + testName();
    long initialSize = cacheCount();
    TableId table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(tableString, table1.canonical());

    // ensure duplicates are not created
    TableId builtInTableId = TableId.of("!0");
    assertSame(MetadataTable.ID, builtInTableId);
    builtInTableId = TableId.of("+r");
    assertSame(RootTable.ID, builtInTableId);
    table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(tableString, table1.canonical());
    TableId table2 = TableId.of(tableString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(tableString, table2.canonical());
    assertSame(table1, table2);
  }

  @Test
  @Timeout(30)
  public void testCacheIncreasesAndDecreasesAfterGC() {
    long initialSize = cacheCount();
    assertTrue(initialSize < 20); // verify initial amount is reasonably low
    LOG.info("Initial cache size: {}", initialSize);
    LOG.info(TableId.cache.asMap().toString());

    // add one and check increase
    String tableString = "table-" + testName();
    TableId table1 = TableId.of(tableString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(tableString, table1.canonical());

    // create a bunch more and throw them away
    long preGCSize = 0;
    int i = 0;
    while ((preGCSize = cacheCount()) < 100) {
      TableId.of(("table" + i++));
    }
    LOG.info("Entries before System.gc(): {}", preGCSize);
    assertEquals(100, preGCSize);
    long postGCSize = preGCSize;
    while (postGCSize >= preGCSize) {
      tryToGc();
      postGCSize = cacheCount();
      LOG.info("Entries after System.gc(): {}", postGCSize);
    }
  }

  @SuppressFBWarnings(value = "DM_GC", justification = "gc is okay for test")
  static void tryToGc() {
    System.gc();
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      fail("Thread interrupted while waiting for GC");
    }
  }
}
