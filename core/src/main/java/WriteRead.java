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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRead {

  private static final Logger log = LoggerFactory.getLogger(WriteRead.class);

  public static void main(String[] args) throws Exception {
    try (var client = Accumulo.newClient().from(args[0]).build()) {
      run(client);
    }
  }

  public static void run(AccumuloClient client) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TableExistsException, InterruptedException {
    if (client.tableOperations().exists("testscan")) {
      client.tableOperations().delete("testscan");
    }

    client.tableOperations().create("testscan");

    try (var writer = client.createBatchWriter("testscan")) {
      for (int i = 0; i < 1000000; i++) {
        Mutation m = new Mutation(String.format("%09d", i));
        m.put("f", "q", i + "");
        writer.addMutation(m);
      }
    }

    client.tableOperations().flush("testscan", null, null, true);

    while (true) {
      long t1 = System.currentTimeMillis();
      try (var scanner = client.createScanner("testscan")) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        scanner.setRange(new Range());
        validate(scanner);
      }

      long t2 = System.currentTimeMillis();

      try (var scanner = client.createBatchScanner("testscan")) {
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
        scanner.setRanges(List.of(new Range()));
        validate(scanner);
      }

      long t3 = System.currentTimeMillis();

      log.info(String.format("Finished scans %.2f %.2f", (t2 - t1) / 1000.0, (t3 - t2) / 1000.0));

      Thread.sleep(100);
    }
  }

  private static void validate(ScannerBase scanner) {
    Set<Integer> seen = new HashSet<>();
    for (Map.Entry<Key,Value> e : scanner) {
      int r = Integer.parseInt(e.getKey().getRowData().toString());
      int v = Integer.parseInt(e.getValue().toString());

      if (r != v) {
        throw new RuntimeException("row != value" + r + " " + v);
      }

      if (r < 0 || r >= 1000000) {
        throw new RuntimeException("unexpected value " + r);
      }

      if (!seen.add(r)) {
        throw new RuntimeException("duplicate seen " + r);
      }

      // TODO could do order check for scanner only
    }

    if (seen.size() != 1000000) {
      throw new RuntimeException("unexpected size " + seen.size());
    }

    for (int i = 0; i < 1000000; i++) {
      if (!seen.contains(i)) {
        throw new RuntimeException("missing " + i);
      }
    }
  }
}
