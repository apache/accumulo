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
package org.apache.accumulo.test.fate.accumulo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.accumulo.AccumuloStore;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloStoreIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(AccumuloStore.class);

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class TestAccumuloStore extends AccumuloStore<String> {
    private final Iterator<Long> tidIterator;

    // use the list of txids to simulate collisions on txids
    public TestAccumuloStore(ClientContext context, String tableName, List<Long> txids) {
      super(context, tableName);
      this.tidIterator = txids.iterator();
    }

    @Override
    public long getTid() {
      if (tidIterator.hasNext()) {
        return tidIterator.next();
      } else {
        return -1L;
      }
    }
  }

  @Test
  public void testCreateWithCollisionAndExceedRetryLimit() throws Exception {
    String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      List<Long> txids = List.of(1L, 1L, 1L, 2L, 3L, 3L, 3L, 3L, 4L, 4L, 5L, 5L, 5L, 5L, 5L, 5L);
      Set<Long> expectedTids = new TreeSet<>(txids);
      TestAccumuloStore store = new TestAccumuloStore(client, table, txids);

      // call create and expect we get the unique txids
      for (Long expectedTid : expectedTids) {
        long tid = store.create();
        log.info("Created tid: " + tid);
        assertEquals(expectedTid, tid, "Expected " + expectedTid + " but got " + tid);
      }

      // Calling create again on 5L should throw an exception since we've exceeded the max retries
      assertThrows(IllegalStateException.class, store::create);
    }
  }
}
