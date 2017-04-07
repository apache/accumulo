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

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.Opts;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BulkIT extends AccumuloClusterHarness {

  private static final int N = 100000;
  private static final int COUNT = 5;
  private static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  private static final ScannerOpts SOPTS = new ScannerOpts();

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private Configuration origConf;

  @Before
  public void saveConf() {
    origConf = CachedConfiguration.getInstance();
  }

  @After
  public void restoreConf() {
    if (null != origConf) {
      CachedConfiguration.setInstance(origConf);
    }
  }

  @Test
  public void test() throws Exception {
    runTest(getConnector(), getCluster().getFileSystem(), getCluster().getTemporaryPath(), getAdminPrincipal(), getUniqueNames(1)[0],
        this.getClass().getName(), testName.getMethodName());
  }

  static void runTest(Connector c, FileSystem fs, Path basePath, String principal, String tableName, String filePrefix, String dirSuffix) throws Exception {
    c.tableOperations().create(tableName);

    Path base = new Path(basePath, "testBulkFail_" + dirSuffix);
    fs.delete(base, true);
    fs.mkdirs(base);
    Path bulkFailures = new Path(base, "failures");
    Path files = new Path(base, "files");
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);

    Opts opts = new Opts();
    opts.timestamp = 1;
    opts.random = 56;
    opts.rows = N;
    opts.instance = c.getInstance().getInstanceName();
    opts.cols = 1;
    opts.setTableName(tableName);
    opts.conf = new Configuration(false);
    opts.fs = fs;
    String fileFormat = filePrefix + "rf%02d";
    for (int i = 0; i < COUNT; i++) {
      opts.outputFile = new Path(files, String.format(fileFormat, i)).toString();
      opts.startRow = N * i;
      TestIngest.ingest(c, fs, opts, BWOPTS);
    }
    opts.outputFile = new Path(files, String.format(fileFormat, N)).toString();
    opts.startRow = N;
    opts.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, fs, opts, BWOPTS);

    // Make sure the server can modify the files
    c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(), false);
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.setTableName(tableName);
    vopts.random = 56;
    vopts.setPrincipal(principal);
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
