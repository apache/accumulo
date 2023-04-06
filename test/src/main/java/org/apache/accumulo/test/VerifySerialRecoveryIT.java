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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class VerifySerialRecoveryIT extends ConfigurableMacBase {

  private static final byte[] HEXCHARS = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
      0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66};

  public static byte[] randomHex(int n) {
    byte[] binary = new byte[n];
    byte[] hex = new byte[n * 2];
    random.nextBytes(binary);
    int count = 0;
    for (byte x : binary) {
      hex[count++] = HEXCHARS[(x >> 4) & 0xf];
      hex[count++] = HEXCHARS[x & 0xf];
    }
    return hex;
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "20");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testSerializedRecovery() throws Exception {
    // make a table with many splits
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      // create splits
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 200; i++) {
        splits.add(new Text(randomHex(8)));
      }

      // create table with config
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      c.tableOperations().create(tableName, ntc);

      // load data to give the recovery something to do
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 50000; i++) {
          Mutation m = new Mutation(randomHex(8));
          m.put("", "", "");
          bw.addMutation(m);
        }
      }
      // kill the tserver
      for (ProcessReference ref : getCluster().getProcesses().get(ServerType.TABLET_SERVER)) {
        getCluster().killProcess(ServerType.TABLET_SERVER, ref);
      }
      final ProcessInfo ts = cluster.exec(TabletServer.class);

      // wait for recovery
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      assertEquals(0, cluster.exec(Admin.class, "stopAll").getProcess().waitFor());
      ts.getProcess().waitFor();
      String result = ts.readStdOut();
      for (String line : result.split("\n")) {
        System.out.println(line);
      }
      // walk through the output, verifying that only a single normal recovery was running at one
      // time
      boolean started = false;
      int recoveries = 0;
      var pattern =
          Pattern.compile(".*recovered \\d+ mutations creating \\d+ entries from \\d+ walogs.*");
      for (String line : result.split("\n")) {
        // ignore metadata tables
        if (line.contains(MetadataTable.ID.canonical())
            || line.contains(RootTable.ID.canonical())) {
          continue;
        }
        if (line.contains("recovering data from walogs")) {
          assertFalse(started);
          started = true;
          recoveries++;
        }
        if (pattern.matcher(line).matches()) {
          assertTrue(started);
          started = false;
        }
      }
      assertFalse(started);
      assertTrue(recoveries > 0);
    }
  }
}
