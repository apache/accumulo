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
package org.apache.accumulo.test.functional;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class SessionDurabilityIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  @Test
  public void nondurableTableHasDurableWrites() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // table default has no durability
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_DURABILITY.getKey(), "none")));
      // send durable writes
      BatchWriterConfig cfg = new BatchWriterConfig();
      cfg.setDurability(Durability.SYNC);
      writeSome(c, tableName, 10, cfg);
      assertEquals(10, count(c, tableName));
      // verify writes servive restart
      restartTServer();
      assertEquals(10, count(c, tableName));
    }
  }

  @Test
  public void durableTableLosesNonDurableWrites() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // table default is durable writes
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_DURABILITY.getKey(), "sync")));
      // write with no durability
      BatchWriterConfig cfg = new BatchWriterConfig();
      cfg.setDurability(Durability.NONE);
      writeSome(c, tableName, 10, cfg);
      // verify writes are lost on restart
      restartTServer();
      assertTrue(count(c, tableName) < 10);
    }
  }

  private int count(AccumuloClient client, String tableName) throws Exception {
    return Iterators.size(client.createScanner(tableName, Authorizations.EMPTY).iterator());
  }

  private void writeSome(AccumuloClient c, String tableName, int n, BatchWriterConfig cfg)
      throws Exception {
    try (BatchWriter bw = c.createBatchWriter(tableName, cfg)) {
      for (int i = 0; i < n; i++) {
        Mutation m = new Mutation(i + "");
        m.put("", "", "");
        bw.addMutation(m);
      }
    }
  }

  @Test
  public void testConditionDurability() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // table default is durable writes
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_DURABILITY.getKey(), "sync")));
      // write without durability
      ConditionalWriterConfig cfg = new ConditionalWriterConfig();
      cfg.setDurability(Durability.NONE);
      conditionWriteSome(c, tableName, 10, cfg);
      // everything in there?
      assertEquals(10, count(c, tableName));
      // restart the server and verify the updates are lost
      restartTServer();
      assertEquals(0, count(c, tableName));
    }
  }

  @Test
  public void testConditionDurability2() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // table default is durable writes
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_DURABILITY.getKey(), "none")));
      // write with durability
      ConditionalWriterConfig cfg = new ConditionalWriterConfig();
      cfg.setDurability(Durability.SYNC);
      conditionWriteSome(c, tableName, 10, cfg);
      // everything in there?
      assertEquals(10, count(c, tableName));
      // restart the server and verify the updates are still there
      restartTServer();
      assertEquals(10, count(c, tableName));
    }
  }

  private void conditionWriteSome(AccumuloClient c, String tableName, int n,
      ConditionalWriterConfig cfg) throws Exception {
    try (ConditionalWriter cw = c.createConditionalWriter(tableName, cfg)) {
      for (int i = 0; i < n; i++) {
        ConditionalMutation m = new ConditionalMutation(i + "", new Condition("", ""));
        m.put("", "", "X");
        assertEquals(Status.ACCEPTED, cw.write(m).getStatus());
      }
    }
  }

  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

}
