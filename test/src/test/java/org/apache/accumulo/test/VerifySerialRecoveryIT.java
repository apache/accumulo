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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class VerifySerialRecoveryIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "3s");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "20");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testSerializedRecovery() throws Exception {
    // make a table with many splits
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 200; i++) {
      splits.add(new Text(AssignmentThreadsIT.randomHex(8)));
    }
    c.tableOperations().addSplits(tableName, splits);
    // load data to give the recovery something to do
    BatchWriter bw = c.createBatchWriter(tableName, null);
    for (int i = 0; i < 50000; i++) {
      Mutation m = new Mutation(AssignmentThreadsIT.randomHex(8));
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
    // kill the tserver
    for (ProcessReference ref : getCluster().getProcesses().get(ServerType.TABLET_SERVER))
      getCluster().killProcess(ServerType.TABLET_SERVER, ref);
    final Process ts = cluster.exec(TabletServer.class);

    // wait for recovery
    Iterators.size(c.createScanner(tableName, Authorizations.EMPTY).iterator());
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
    ts.waitFor();
    String result = FunctionalTestUtils.readAll(cluster, TabletServer.class, ts);
    for (String line : result.split("\n")) {
      System.out.println(line);
    }
    // walk through the output, verifying that only a single normal recovery was running at one time
    boolean started = false;
    int recoveries = 0;
    for (String line : result.split("\n")) {
      // ignore metadata tables
      if (line.contains("!0") || line.contains("+r"))
        continue;
      if (line.contains("Starting Write-Ahead Log")) {
        assertFalse(started);
        started = true;
        recoveries++;
      }
      if (line.contains("Write-Ahead Log recovery complete")) {
        assertTrue(started);
        started = false;
      }
    }
    assertFalse(started);
    assertTrue(recoveries > 0);
  }
}
