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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class CleanWalIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(CleanWalIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
    cfg.setNumTservers(1);
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  // test for ACCUMULO-1830
  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", "value");
        bw.addMutation(m);
      }
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      // all 3 tables should do recovery, but the bug doesn't really remove the log file references

      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);

      for (String table : new String[] {MetadataTable.NAME, RootTable.NAME}) {
        client.tableOperations().flush(table, null, null, true);
      }
      log.debug("Checking entries for {}", tableName);
      assertEquals(1, count(tableName, client));
      for (String table : new String[] {MetadataTable.NAME, RootTable.NAME}) {
        log.debug("Checking logs for {}", table);
        assertEquals(0, countLogs(client), "Found logs for " + table);
      }

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("row");
        m.putDelete("cf", "cq");
        bw.addMutation(m);
      }
      assertEquals(0, count(tableName, client));
      client.tableOperations().flush(tableName, null, null, true);
      client.tableOperations().flush(MetadataTable.NAME, null, null, true);
      client.tableOperations().flush(RootTable.NAME, null, null, true);
      try {
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        sleepUninterruptibly(3, TimeUnit.SECONDS);
      } finally {
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
      assertEquals(0, count(tableName, client));
    }
  }

  private int countLogs(AccumuloClient client) throws TableNotFoundException {
    int count = 0;
    try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      scanner.fetchColumnFamily(LogColumnFamily.NAME);
      scanner.setRange(TabletsSection.getRange());
      for (Entry<Key,Value> entry : scanner) {
        log.debug("Saw {}={}", entry.getKey(), entry.getValue());
        count++;
      }
    }
    return count;
  }

  int count(String tableName, AccumuloClient client) throws Exception {
    try (Scanner s = client.createScanner(tableName, Authorizations.EMPTY)) {
      return Iterators.size(s.iterator());
    }
  }
}
