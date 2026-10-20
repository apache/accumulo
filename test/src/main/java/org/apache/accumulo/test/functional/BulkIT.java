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

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.GsonBuilder;

/**
 * Tests Old and New Bulk import
 */
@Tag(SUNNY_DAY)
public class BulkIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(BulkIT.class);

  private static final int N = 100000;
  private static final int COUNT = 5;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  public static void setBulkSourceLocations(AccumuloClient client, FileSystem fs, Path basePath)
      throws AccumuloException, AccumuloSecurityException {
    Map<String,String> userPatterns = new HashMap<>();
    userPatterns.put(client.whoami(), fs.makeQualified(basePath).toString() + "/.*");
    String propVal = new GsonBuilder().create().toJson(userPatterns);
    LOG.info("Property set to {}", propVal);
    client.instanceOperations().setProperty(Property.GENERAL_BULK_SOURCE_DIRS.getKey(), propVal);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Path basePath = getCluster().getTemporaryPath();
      FileSystem fs = getCluster().getFileSystem();
      setBulkSourceLocations(client, fs, basePath);
      runTest(client, fs, basePath, getUniqueNames(1)[0], this.getClass().getName(), testName(),
          false);
    }
  }

  @Test
  public void testOld() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Path basePath = getCluster().getTemporaryPath();
      FileSystem fs = getCluster().getFileSystem();
      setBulkSourceLocations(client, fs, basePath);
      runTest(client, fs, basePath, getUniqueNames(1)[0], this.getClass().getName(), testName(),
          true);
    }
  }

  @Test
  public void testWithoutSourceDirProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Path basePath = getCluster().getTemporaryPath();
      FileSystem fs = getCluster().getFileSystem();
      AccumuloSecurityException ase =
          assertThrows(AccumuloSecurityException.class, () -> runTest(client, fs, basePath,
              getUniqueNames(1)[0], this.getClass().getName(), testName(), false));
      assertTrue(ase.getMessage().startsWith("Error PERMISSION_DENIED for user root on table "));
      assertTrue(ase.getMessage().endsWith("User does not have permission to perform this action"));
    }
  }

  @Test
  public void testOldWithoutSourceDirProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Path basePath = getCluster().getTemporaryPath();
      FileSystem fs = getCluster().getFileSystem();
      AccumuloSecurityException ase =
          assertThrows(AccumuloSecurityException.class, () -> runTest(client, fs, basePath,
              getUniqueNames(1)[0], this.getClass().getName(), testName(), true));
      assertTrue(ase.getMessage().startsWith("Error PERMISSION_DENIED for user root on table "));
      assertTrue(ase.getMessage().endsWith("User does not have permission to perform this action"));
    }
  }

  static void runTest(AccumuloClient c, FileSystem fs, Path basePath, String tableName,
      String filePrefix, String dirSuffix, boolean useOld) throws Exception {
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

    IngestParams params = new IngestParams(c.properties(), tableName, N);
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
    VerifyParams verifyParams = new VerifyParams(c.properties(), tableName, N);
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
      // not appending the 'ignoreEmptyDir' method defaults to not ignoring empty directories.
      c.tableOperations().importDirectory(files.toString()).to(tableName).load();
      try {
        // if run again, the expected IllegalArgrumentException is thrown
        c.tableOperations().importDirectory(files.toString()).to(tableName).load();
      } catch (IllegalArgumentException ex) {
        // expected exception to be thrown
      }
      // re-run using the ignoreEmptyDir option and no error should be thrown since empty
      // directories will be ignored
      c.tableOperations().importDirectory(files.toString()).to(tableName).ignoreEmptyDir(true)
          .load();
      try {
        // setting ignoreEmptyDir to false, explicitly, results in exception being thrown again.
        c.tableOperations().importDirectory(files.toString()).to(tableName).ignoreEmptyDir(false)
            .load();
      } catch (IllegalArgumentException ex) {
        // expected exception to be thrown
      }
    }
  }
}
