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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests the Table ID class, mainly the internal cache.
 */
public class TableTest {
  @Rule
  public TestName name = new TestName();

  @Test
  public void testCacheIncreases() {
    Long initialSize = Table.ID.cache.asMap().entrySet().stream().count();
    String tableString = "table-" + name.getMethodName();
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonicalID());
  }

  @Test
  public void testCacheNoDuplicates() {
    String tableString = "table-" + name.getMethodName();
    Long initialSize = Table.ID.cache.asMap().entrySet().stream().count();
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonicalID());

    // ensure duplicates are not created
    Table.ID builtInTableId = Table.ID.of("!0");
    assertSame(Table.ID.METADATA, builtInTableId);
    builtInTableId = Table.ID.of("+r");
    assertSame(Table.ID.ROOT, builtInTableId);
    builtInTableId = Table.ID.of("+rep");
    assertSame(Table.ID.REPLICATION, builtInTableId);
    table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table1.canonicalID());
    Table.ID table2 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().entrySet().stream().count());
    assertEquals(tableString, table2.canonicalID());
    assertSame(table1, table2);
  }

  @Test(timeout = 60_000)
  public void testCacheDecreasesAfterGC() {
    Long initialSize = Table.ID.cache.asMap().entrySet().stream().count();
    generateJunkCacheEntries();
    Long postGCSize;
    do {
      System.gc();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
      }
      postGCSize = Table.ID.cache.asMap().entrySet().stream().count();
    } while (postGCSize > initialSize);

    assertTrue("Cache did not decrease with GC.", Table.ID.cache.asMap().entrySet().stream().count() < initialSize);
  }

  private void generateJunkCacheEntries() {
    for (int i = 0; i < 1000; i++)
      Table.ID.of(new String("table" + i));
  }

}
