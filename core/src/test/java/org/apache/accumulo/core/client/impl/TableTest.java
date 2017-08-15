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

import org.junit.Test;

/**
 * Tests the Table ID class, mainly the internal cache.
 */
public class TableTest {

  @Test
  public void testCacheIncreases() {
    Integer initialSize = Table.ID.cache.asMap().size();

    String tableString = "table-testWeakHashMapIncreases";
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().size());
    assertEquals(tableString, table1.canonicalID());
  }

  @Test
  public void testCacheNoDuplicates() {
    String tableString = "table-testWeakHashMapNoDuplicates";
    Integer initialSize = Table.ID.cache.asMap().size();
    Table.ID table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().size());
    assertEquals(tableString, table1.canonicalID());

    // ensure duplicates are not created
    Table.ID builtInTableId = Table.ID.of("!0");
    assertEquals(Table.ID.METADATA, builtInTableId);
    builtInTableId = Table.ID.of("+r");
    assertEquals(Table.ID.ROOT, builtInTableId);
    builtInTableId = Table.ID.of("+rep");
    assertEquals(Table.ID.REPLICATION, builtInTableId);
    table1 = Table.ID.of(tableString);
    assertEquals(initialSize + 1, Table.ID.cache.asMap().size());
    assertEquals(tableString, table1.canonicalID());
    table1 = Table.ID.of("table-testWeakHashMapNoDuplicates");
    assertEquals(initialSize + 1, Table.ID.cache.asMap().size());
    assertEquals(tableString, table1.canonicalID());
  }

}
