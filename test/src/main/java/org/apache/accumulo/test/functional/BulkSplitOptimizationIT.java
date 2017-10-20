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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ClientOpts.Password;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test verifies that when a lot of files are bulk imported into a table with one tablet and then splits that not all map files go to the children tablets.
 */

public class BulkSplitOptimizationIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1s");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  private String majcDelay;

  @Before
  public void alterConfig() throws Exception {
    Connector conn = getConnector();
    majcDelay = conn.instanceOperations().getSystemConfiguration().get(Property.TSERV_MAJC_DELAY.getKey());
    if (!"1s".equals(majcDelay)) {
      conn.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1s");
      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  @After
  public void resetConfig() throws Exception {
    if (null != majcDelay) {
      Connector conn = getConnector();
      conn.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
  }

  static final int ROWS = 100000;
  static final int SPLITS = 99;

  @Test
  public void testBulkSplitOptimization() throws Exception {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1000");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "1000");
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "1G");
    FileSystem fs = cluster.getFileSystem();
    Path testDir = new Path(getUsableDir(), "testmf");
    FunctionalTestUtils.createRFiles(c, fs, testDir.toString(), ROWS, SPLITS, 8);
    FileStatus[] stats = fs.listStatus(testDir);

    System.out.println("Number of generated files: " + stats.length);
    FunctionalTestUtils.bulkImport(c, fs, tableName, testDir.toString());
    FunctionalTestUtils.checkSplits(c, tableName, 0, 0);
    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 100, 100);

    // initiate splits
    getConnector().tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "100K");

    sleepUninterruptibly(2, TimeUnit.SECONDS);

    // wait until over split threshold -- should be 78 splits
    while (getConnector().tableOperations().listSplits(tableName).size() < 75) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }

    FunctionalTestUtils.checkSplits(c, tableName, 50, 100);
    VerifyIngest.Opts opts = new VerifyIngest.Opts();
    opts.timestamp = 1;
    opts.dataSize = 50;
    opts.random = 56;
    opts.rows = 100000;
    opts.startRow = 0;
    opts.cols = 1;
    opts.setTableName(tableName);

    AuthenticationToken adminToken = getAdminToken();
    if (adminToken instanceof PasswordToken) {
      PasswordToken token = (PasswordToken) getAdminToken();
      opts.setPassword(new Password(new String(token.getPassword(), UTF_8)));
      opts.setPrincipal(getAdminPrincipal());
    } else if (adminToken instanceof KerberosToken) {
      ClientConfiguration clientConf = cluster.getClientConfig();
      opts.updateKerberosCredentials(clientConf);
    } else {
      Assert.fail("Unknown token type");
    }

    VerifyIngest.verifyIngest(c, opts, new ScannerOpts());

    // ensure each tablet does not have all map files, should be ~2.5 files per tablet
    FunctionalTestUtils.checkRFiles(c, tableName, 50, 100, 1, 4);
  }

}
