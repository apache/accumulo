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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class RestartIT extends ConfigurableMacIT {
  @Override
  public int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "5s");
    props.put(Property.GC_CYCLE_DELAY.getKey(), "1s");
    props.put(Property.GC_CYCLE_START.getKey(), "1s");
    cfg.setSiteConfig(props);
    cfg.useMiniDFS(true);
  }

  private static final ScannerOpts SOPTS = new ScannerOpts();
  private static final VerifyIngest.Opts VOPTS = new VerifyIngest.Opts();
  private static final TestIngest.Opts OPTS = new TestIngest.Opts();
  private static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  static {
    OPTS.rows = VOPTS.rows = 10 * 1000;
  }

  @Test
  public void restartMaster() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    Process ingest = cluster.exec(TestIngest.class, "-u", "root", "-p", ROOT_PASSWORD, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(),
        "--rows", "" + OPTS.rows);
    for (ProcessReference master : cluster.getProcesses().get(ServerType.MASTER)) {
      cluster.killProcess(ServerType.MASTER, master);
    }
    cluster.start();
    assertEquals(0, ingest.waitFor());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    ingest.destroy();
  }

  @Test
  public void restartMasterRecovery() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    TestIngest.ingest(c, OPTS, BWOPTS);
    for (Entry<ServerType,Collection<ProcessReference>> entry : cluster.getProcesses().entrySet()) {
      for (ProcessReference proc : entry.getValue()) {
        cluster.killProcess(entry.getKey(), proc);
      }
    }
    cluster.start();
    UtilWaitThread.sleep(5);
    for (ProcessReference master : cluster.getProcesses().get(ServerType.MASTER)) {
      cluster.killProcess(ServerType.MASTER, master);
    }
    cluster.start();
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void restartMasterSplit() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");
    Process ingest = cluster.exec(TestIngest.class, "-u", "root", "-p", ROOT_PASSWORD, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(),
        "--rows", "" + VOPTS.rows);
    for (ProcessReference master : cluster.getProcesses().get(ServerType.MASTER)) {
      cluster.killProcess(ServerType.MASTER, master);
    }
    cluster.start();
    assertEquals(0, ingest.waitFor());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    ingest.destroy();
  }

  @Test
  public void killedTabletServer() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    TestIngest.ingest(c, OPTS, BWOPTS);
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    List<ProcessReference> procs = new ArrayList<ProcessReference>(cluster.getProcesses().get(ServerType.TABLET_SERVER));
    for (ProcessReference tserver : procs) {
      cluster.killProcess(ServerType.TABLET_SERVER, tserver);
    }
    cluster.start();
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void killedTabletServer2() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("t");
    Process gc = cluster.exec(SimpleGarbageCollector.class);
    UtilWaitThread.sleep(5 * 1000);
    gc.destroy();
    List<ProcessReference> procs = new ArrayList<ProcessReference>(cluster.getProcesses().get(ServerType.TABLET_SERVER));
    for (ProcessReference tserver : procs) {
      cluster.killProcess(ServerType.TABLET_SERVER, tserver);
    }
    cluster.start();
    c.tableOperations().create("tt");
  }

  @Test
  public void killedTabletServerDuringShutdown() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    TestIngest.ingest(c, OPTS, BWOPTS);
    List<ProcessReference> procs = new ArrayList<ProcessReference>(cluster.getProcesses().get(ServerType.TABLET_SERVER));
    cluster.killProcess(ServerType.TABLET_SERVER, procs.get(0));
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }

  @Test
  public void shutdownDuringCompactingSplitting() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "20K");
    TestIngest.Opts opts = new TestIngest.Opts();
    TestIngest.ingest(c, opts, BWOPTS);
    c.tableOperations().flush("test_ingest", null, null, false);
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }
}
