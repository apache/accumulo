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

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModifyTable {

  private static final Logger log = LoggerFactory.getLogger(ModifyTable.class);

  public static void main(String[] args) throws Exception {
    try (var client = Accumulo.newClient().from(args[0]).build()) {
      run(client);
    }
  }

  public static void run(AccumuloClient client) throws Exception {
    Random rand = new Random();

    while (true) {
      var p = rand.nextDouble();

      if (p < .05) {
        switchTableState(client);
      } else if (p < .3666) {
        splitTable(client, rand);
      } else if (p < .6833) {
        mergeTable(client, rand);
      } else {
        compactTable(client);
      }

      Thread.sleep(5000);
    }
  }

  private static void compactTable(AccumuloClient client) throws Exception {
    boolean takeOffline = false;

    if (!client.tableOperations().isOnline("testscan")) {
      client.tableOperations().online("testscan", true);
      takeOffline = true;
    }

    client.tableOperations().compact("testscan", new CompactionConfig().setWait(true));

    log.info("Compacted table");

    if (takeOffline) {
      client.tableOperations().offline("testscan", true);
    }
  }

  private static void mergeTable(AccumuloClient client, Random rand) throws Exception {

    var splits = new TreeSet<>(client.tableOperations().listSplits("testscan"));

    if (splits.isEmpty()) {
      log.info("Table has not splits, so not merging");
      return;
    }

    boolean takeOffline = false;

    if (!client.tableOperations().isOnline("testscan")) {
      client.tableOperations().online("testscan", true);
      takeOffline = true;
    }

    int d = rand.nextInt(4) + 1;

    // remove a random subset of splits
    splits.removeIf(split -> split.hashCode() % d == 0);

    if (splits.size() <= 1) {
      client.tableOperations().merge("testscan", null, null);
      log.info("Merged entire table");
    } else {
      var begin = splits.first();
      var end = splits.last();

      client.tableOperations().merge("testscan", begin, end);

      log.info("Merged {} to {}", begin, end);
    }

    if (takeOffline) {
      client.tableOperations().offline("testscan", true);
    }
  }

  private static void splitTable(AccumuloClient client, Random rand) throws Exception {
    boolean takeOffline = false;
    if (!client.tableOperations().isOnline("testscan")) {
      client.tableOperations().online("testscan", true);
      takeOffline = true;
    }

    int numToAdd = rand.nextInt(5) + 1;

    SortedSet<Text> splits = new TreeSet<>();

    for (int i = 0; i < numToAdd; i++) {
      splits.add(new Text(String.format("%09d", rand.nextInt(1000000))));
    }

    client.tableOperations().addSplits("testscan", splits);
    log.info("Added splits : {}", splits);

    if (takeOffline) {
      client.tableOperations().offline("testscan", true);
    }
  }

  private static void switchTableState(AccumuloClient client) throws Exception {
    if (client.tableOperations().isOnline("testscan")) {
      client.tableOperations().offline("testscan", true);
      log.info("Took table offline");
    } else {
      client.tableOperations().online("testscan", true);
      log.info("Brought table online");
    }
  }
}
