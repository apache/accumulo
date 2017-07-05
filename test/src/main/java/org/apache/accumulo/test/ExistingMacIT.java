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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.util.AccumuloStatus;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

public class ExistingMacIT extends ConfigurableMacBase {
  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    // NativeMap.java was changed to fail if native lib missing in ACCUMULO-4596
    // testExistingInstance will fail because the native path is not set in MiniAccumuloConfigImpl.useExistingInstance
    // so disable Native maps for this test
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, "false");

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private void createEmptyConfig(File confFile) throws IOException {
    Configuration conf = new Configuration(false);
    OutputStream hcOut = new FileOutputStream(confFile);
    conf.writeXml(hcOut);
    hcOut.close();
  }

  @Test
  public void testExistingInstance() throws Exception {

    Connector conn = getCluster().getConnector("root", new PasswordToken(ROOT_PASSWORD));

    conn.tableOperations().create("table1");

    BatchWriter bw = conn.createBatchWriter("table1", new BatchWriterConfig());

    Mutation m1 = new Mutation("00081");
    m1.put("math", "sqroot", "9");
    m1.put("math", "sq", "6560");

    bw.addMutation(m1);
    bw.close();

    conn.tableOperations().flush("table1", null, null, true);
    // TOOD use constants
    conn.tableOperations().flush(MetadataTable.NAME, null, null, true);
    conn.tableOperations().flush(RootTable.NAME, null, null, true);

    Set<Entry<ServerType,Collection<ProcessReference>>> procs = getCluster().getProcesses().entrySet();
    for (Entry<ServerType,Collection<ProcessReference>> entry : procs) {
      if (entry.getKey() == ServerType.ZOOKEEPER)
        continue;
      for (ProcessReference pr : entry.getValue())
        getCluster().killProcess(entry.getKey(), pr);
    }

    final DefaultConfiguration defaultConfig = DefaultConfiguration.getInstance();
    final long zkTimeout = ConfigurationTypeHelper.getTimeInMillis(getCluster().getConfig().getSiteConfig().get(Property.INSTANCE_ZK_TIMEOUT.getKey()));
    IZooReaderWriter zrw = new ZooReaderWriterFactory().getZooReaderWriter(getCluster().getZooKeepers(), (int) zkTimeout,
        defaultConfig.get(Property.INSTANCE_SECRET));
    final String zInstanceRoot = Constants.ZROOT + "/" + conn.getInstance().getInstanceID();
    while (!AccumuloStatus.isAccumuloOffline(zrw, zInstanceRoot)) {
      log.debug("Accumulo services still have their ZK locks held");
      Thread.sleep(1000);
    }

    File hadoopConfDir = createTestDir(ExistingMacIT.class.getSimpleName() + "_hadoop_conf");
    FileUtils.deleteQuietly(hadoopConfDir);
    assertTrue(hadoopConfDir.mkdirs());
    createEmptyConfig(new File(hadoopConfDir, "core-site.xml"));
    createEmptyConfig(new File(hadoopConfDir, "hdfs-site.xml"));

    File testDir2 = createTestDir(ExistingMacIT.class.getSimpleName() + "_2");
    FileUtils.deleteQuietly(testDir2);

    MiniAccumuloConfigImpl macConfig2 = new MiniAccumuloConfigImpl(testDir2, "notused");
    macConfig2.useExistingInstance(new File(getCluster().getConfig().getConfDir(), "accumulo-site.xml"), hadoopConfDir);

    MiniAccumuloClusterImpl accumulo2 = new MiniAccumuloClusterImpl(macConfig2);
    accumulo2.start();

    conn = accumulo2.getConnector("root", new PasswordToken(ROOT_PASSWORD));

    Scanner scanner = conn.createScanner("table1", Authorizations.EMPTY);

    int sum = 0;
    for (Entry<Key,Value> entry : scanner) {
      sum += Integer.parseInt(entry.getValue().toString());
    }

    Assert.assertEquals(6569, sum);

    accumulo2.stop();
  }

  @Test
  public void testExistingRunningInstance() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector conn = getConnector();
    // Ensure that a master and tserver are up so the existing instance check won't fail.
    conn.tableOperations().create(table);
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation("foo");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    File hadoopConfDir = createTestDir(ExistingMacIT.class.getSimpleName() + "_hadoop_conf_2");
    FileUtils.deleteQuietly(hadoopConfDir);
    assertTrue(hadoopConfDir.mkdirs());
    createEmptyConfig(new File(hadoopConfDir, "core-site.xml"));
    createEmptyConfig(new File(hadoopConfDir, "hdfs-site.xml"));

    File testDir2 = createTestDir(ExistingMacIT.class.getSimpleName() + "_3");
    FileUtils.deleteQuietly(testDir2);

    MiniAccumuloConfigImpl macConfig2 = new MiniAccumuloConfigImpl(testDir2, "notused");
    macConfig2.useExistingInstance(new File(getCluster().getConfig().getConfDir(), "accumulo-site.xml"), hadoopConfDir);

    System.out.println("conf " + new File(getCluster().getConfig().getConfDir(), "accumulo-site.xml"));

    MiniAccumuloClusterImpl accumulo2 = new MiniAccumuloClusterImpl(macConfig2);
    try {
      accumulo2.start();
      Assert.fail("A 2nd MAC instance should not be able to start over an existing MAC instance");
    } catch (RuntimeException e) {
      // TODO check message or throw more explicit exception
    }
  }
}
