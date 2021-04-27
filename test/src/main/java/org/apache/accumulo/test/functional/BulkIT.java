/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests Old and New Bulk import
 */
public class BulkIT extends AccumuloClusterHarness {

  private static final int N = 100000;
  private static final int COUNT = 5;

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      runTest(client, getClientInfo(), getCluster().getFileSystem(),
          getCluster().getTemporaryPath(), getUniqueNames(1)[0], this.getClass().getName(),
          testName.getMethodName(), false);
    }
  }

  @Test
  public void testOld() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      runTest(client, getClientInfo(), getCluster().getFileSystem(),
          getCluster().getTemporaryPath(), getUniqueNames(1)[0], this.getClass().getName(),
          testName.getMethodName(), true);
    }
  }

  static void runTest(AccumuloClient c, ClientInfo info, FileSystem fs, Path basePath,
      String tableName, String filePrefix, String dirSuffix, boolean useOld) throws Exception {
    c.tableOperations().create(tableName);
    Path base = new Path(basePath, "testBulkFail_" + dirSuffix);
    fs.delete(base, true);
    fs.mkdirs(base);
    fs.deleteOnExit(base);
    Path bulkFailures = new Path(base, "failures");
    fs.deleteOnExit(bulkFailures);
    Path files = new Path(base, "files");
    fs.deleteOnExit(files);
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);

    IngestParams params = new IngestParams(info.getProperties(), tableName, N);
    params.timestamp = 1;
    params.random = 56;
    params.cols = 1;
    String fileFormat = filePrefix + "rf%02d";
    for (int i = 0; i < COUNT; i++) {
      params.outputFile = new Path(files, String.format(fileFormat, i)).toString();
      params.startRow = N * i;
      TestIngest.ingest(c, fs, params);
    }
    params.outputFile = new Path(files, String.format(fileFormat, N)).toString();
    params.startRow = N;
    params.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, fs, params);

    bulkLoad(c, tableName, bulkFailures, files, useOld);
    VerifyParams verifyParams = new VerifyParams(info.getProperties(), tableName, N);
    verifyParams.random = 56;
    for (int i = 0; i < COUNT; i++) {
      verifyParams.startRow = i * N;
      VerifyIngest.verifyIngest(c, verifyParams);
    }
    verifyParams.startRow = N;
    verifyParams.rows = 1;
    VerifyIngest.verifyIngest(c, verifyParams);
  }

  @SuppressWarnings("deprecation")
  private static void bulkLoad(AccumuloClient c, String tableName, Path bulkFailures, Path files,
      boolean useOld)
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {
    // Make sure the server can modify the files
    if (useOld) {
      c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(),
          false);
    } else {
      c.tableOperations().importDirectory(files.toString()).to(tableName).load();
      // rerun using the ignore option and no error should be thrown since empty directories
      // will not throw an exception.
      c.tableOperations().importDirectory(files.toString(), true).to(tableName).load();
      try {
        // if run again, without ignore flag, an IllegalArgrumentException should be thrown
        c.tableOperations().importDirectory(files.toString(), false).to(tableName).load();
      } catch (IllegalArgumentException ex) {
        // expected exception to be thrown
      }
    }
  }
}
