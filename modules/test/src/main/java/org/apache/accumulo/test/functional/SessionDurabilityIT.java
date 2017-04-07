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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class SessionDurabilityIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  @Test(timeout = 3 * 60 * 1000)
  public void nondurableTableHasDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default has no durability
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
    // send durable writes
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.SYNC);
    writeSome(tableName, 10, cfg);
    assertEquals(10, count(tableName));
    // verify writes servive restart
    restartTServer();
    assertEquals(10, count(tableName));
  }

  @Test(timeout = 3 * 60 * 1000)
  public void durableTableLosesNonDurableWrites() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default is durable writes
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
    // write with no durability
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setDurability(Durability.NONE);
    writeSome(tableName, 10, cfg);
    // verify writes are lost on restart
    restartTServer();
    assertTrue(10 > count(tableName));
  }

  private int count(String tableName) throws Exception {
    return Iterators.size(getConnector().createScanner(tableName, Authorizations.EMPTY).iterator());
  }

  private void writeSome(String tableName, int n, BatchWriterConfig cfg) throws Exception {
    Connector c = getConnector();
    BatchWriter bw = c.createBatchWriter(tableName, cfg);
    for (int i = 0; i < n; i++) {
      Mutation m = new Mutation(i + "");
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
  }

  @Test(timeout = 3 * 60 * 1000)
  public void testConditionDurability() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default is durable writes
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "sync");
    // write without durability
    ConditionalWriterConfig cfg = new ConditionalWriterConfig();
    cfg.setDurability(Durability.NONE);
    conditionWriteSome(tableName, 10, cfg);
    // everything in there?
    assertEquals(10, count(tableName));
    // restart the server and verify the updates are lost
    restartTServer();
    assertEquals(0, count(tableName));
  }

  @Test(timeout = 3 * 60 * 1000)
  public void testConditionDurability2() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    // table default is durable writes
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_DURABILITY.getKey(), "none");
    // write with durability
    ConditionalWriterConfig cfg = new ConditionalWriterConfig();
    cfg.setDurability(Durability.SYNC);
    conditionWriteSome(tableName, 10, cfg);
    // everything in there?
    assertEquals(10, count(tableName));
    // restart the server and verify the updates are still there
    restartTServer();
    assertEquals(10, count(tableName));
  }

  private void conditionWriteSome(String tableName, int n, ConditionalWriterConfig cfg) throws Exception {
    Connector c = getConnector();
    ConditionalWriter cw = c.createConditionalWriter(tableName, cfg);
    for (int i = 0; i < n; i++) {
      ConditionalMutation m = new ConditionalMutation((CharSequence) (i + ""), new Condition("", ""));
      m.put("", "", "X");
      assertEquals(Status.ACCEPTED, cw.write(m).getStatus());
    }
  }

  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

}
