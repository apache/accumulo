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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.jupiter.api.Test;

public class CloseScannerIT extends AccumuloClusterHarness {

  static final int ROWS = 1000;
  static final int COLS = 1000;

  @Test
  public void testManyScans() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, ROWS, COLS, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      for (int i = 0; i < 200; i++) {
        try (Scanner scanner = createScanner(client, tableName, i)) {
          scanner.setRange(new Range());
          scanner.setReadaheadThreshold(i % 2 == 0 ? 0 : 3);

          for (int j = 0; j < i % 7 + 1; j++) {
            // only read a little data and quit, this should leave a session open on the tserver
            scanner.stream().limit(10).forEach(e -> {});
          }
        } // when the scanner is closed, all open sessions should be closed
      }

      List<String> tservers = client.instanceOperations().getTabletServers();
      int activeScans = 0;
      for (String tserver : tservers) {
        activeScans += client.instanceOperations().getActiveScans(tserver).size();
      }

      assertTrue(activeScans < 3);
    }
  }

  private static Scanner createScanner(AccumuloClient client, String tableName, int i)
      throws Exception {
    Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
    if (i % 2 == 0) {
      scanner = new IsolatedScanner(scanner);
    }
    return scanner;
  }
}
