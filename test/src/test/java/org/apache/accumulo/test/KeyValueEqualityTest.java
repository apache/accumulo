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
package org.apache.accumulo.test;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KeyValueEqualityTest {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static final PasswordToken password = new PasswordToken("secret");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), new String(password.getPassword()));
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Test
  public void testEquality() throws Exception {
    final ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    final Connector conn = instance.getConnector("root", password);
    final BatchWriterConfig config = new BatchWriterConfig();

    final String table1 = "table1", table2 = "table2";
    final TableOperations tops = conn.tableOperations();
    tops.create(table1);
    tops.create(table2);

    final BatchWriter bw1 = conn.createBatchWriter(table1, config), bw2 = conn.createBatchWriter(table2, config);

    for (int row = 0; row < 100; row++) {
      Mutation m = new Mutation(Integer.toString(row));
      for (int col = 0; col < 10; col++) {
        m.put(Integer.toString(col), "", System.currentTimeMillis(), Integer.toString(col * 2));
      }
      bw1.addMutation(m);
      bw2.addMutation(m);
    }

    bw1.close();
    bw2.close();

    Iterator<Entry<Key,Value>> t1 = conn.createScanner(table1, Authorizations.EMPTY).iterator(), t2 = conn.createScanner(table2, Authorizations.EMPTY).iterator();
    while (t1.hasNext() && t2.hasNext()) {
      // KeyValue, the implementation of Entry<Key,Value>, should support equality and hashCode properly
      Entry<Key,Value> e1 = t1.next(), e2 = t2.next();
      Assert.assertEquals(e1, e2);
      Assert.assertEquals(e1.hashCode(), e2.hashCode());
    }
    Assert.assertFalse("table1 had more data to read", t1.hasNext());
    Assert.assertFalse("table2 had more data to read", t2.hasNext());
  }
}
