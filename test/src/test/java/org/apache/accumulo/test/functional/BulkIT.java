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

import java.io.IOException;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.harness.AccumuloIT;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.Opts;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// TODO Change test to support bulk ingest on any filesystem, not just the local FS.
public class BulkIT extends AccumuloIT {

  private static final int N = 100000;
  private static final int COUNT = 5;
  private static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  private static final ScannerOpts SOPTS = new ScannerOpts();

  private MiniAccumuloClusterImpl cluster;

  private AuthenticationToken getToken() {
    return new PasswordToken("rootPassword1");
  }

  private Connector getConnector() {
    try {
      return cluster.getConnector("root", getToken());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Before
  public void startMiniCluster() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();
    cluster = harness.create(getToken());
    cluster.start();
  }

  @After
  public void stopMiniCluster() throws Exception {
    cluster.stop();
  }

  @Test
  public void test() throws Exception {
    runTest(getConnector(), getUniqueNames(1)[0], this.getClass().getName(), testName.getMethodName());
  }

  static void runTest(Connector c, String tableName, String filePrefix, String dirSuffix) throws AccumuloException, AccumuloSecurityException,
      TableExistsException, IOException, TableNotFoundException, MutationsRejectedException {
    c.tableOperations().create(tableName);
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    String base = "target/accumulo-maven-plugin";
    String bulkFailures = base + "/testBulkFail_" + dirSuffix;
    fs.delete(new Path(base + "/testrf"), true);
    fs.mkdirs(new Path(bulkFailures));

    Opts opts = new Opts();
    opts.timestamp = 1;
    opts.random = 56;
    opts.rows = N;
    opts.instance = c.getInstance().getInstanceName();
    opts.cols = 1;
    opts.tableName = tableName;
    String fileFormat = "/testrf/" + filePrefix + "rf%02d";
    for (int i = 0; i < COUNT; i++) {
      opts.outputFile = base + String.format(fileFormat, i);
      opts.startRow = N * i;
      TestIngest.ingest(c, opts, BWOPTS);
    }
    opts.outputFile = base + String.format(fileFormat, N);
    opts.startRow = N;
    opts.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, opts, BWOPTS);
    c.tableOperations().importDirectory(tableName, base + "/testrf", bulkFailures, false);
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.tableName = tableName;
    vopts.random = 56;
    for (int i = 0; i < COUNT; i++) {
      vopts.startRow = i * N;
      vopts.rows = N;
      VerifyIngest.verifyIngest(c, vopts, SOPTS);
    }
    vopts.startRow = N;
    vopts.rows = 1;
    VerifyIngest.verifyIngest(c, vopts, SOPTS);
  }

}
