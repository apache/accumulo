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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CreateTableIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testCreateLotsOfTables() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(500);

      for (int i = 0; i < tableNames.length; i++) {
        // Create waits for the Fate operation to complete
        long start = System.currentTimeMillis();
        client.tableOperations().create(tableNames[i]);
        System.out.println("Table creation took: " + (System.currentTimeMillis() - start) + "ms");
      }
      // Confirm all 500 user tables exist in addition to Root, Metadata,
      // and ScanRef tables
      assertEquals(503, client.tableOperations().list().size());
    }
  }

}
