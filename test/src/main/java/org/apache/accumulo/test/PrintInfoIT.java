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
package org.apache.accumulo.test;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PrintInfoIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @TempDir
  private static File tempDir;

  // Create an RFile that does not contain any summary info. Call PrintInfo/rfile-info on the RFile
  // twice.
  // The first time, the --summary argument is not used so no attempt is made to find summary info.
  // The second call uses the --summary argument. Since no summary info is available, verify that no
  // exception is
  // thrown. Instead, a line is output indicating that no summary data is present in the RFile.
  @Test
  public void testRFileNotContainingSummaryInfo() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      createTableAndFlush(accumuloClient, table, false);
      String rFileName = getRFileName(accumuloClient, table);

      String output = execPrintInfo(rFileName, false);
      assertTrue(output.contains("RFile Version            : 8"));
      assertFalse(output.contains("Meta block     : accumulo.summaries.index"));
      assertFalse(output.contains("No summary data present in file"));

      output = execPrintInfo(rFileName, true);
      assertTrue(output.contains("RFile Version            : 8"));
      assertFalse(output.contains("Meta block     : accumulo.summaries.index"));
      assertTrue(output.contains("No summary data present in file"));
    }
  }

  // Create an RFile that contains summary info. Call PrintInfo/rfile-info on the RFile twice.
  // The first time, the --summary argument is not used so no attempt is made to find summary info.
  // The second call uses the --summary argument. Verify that the summary info if output.
  @Test
  public void testRFileContainingSummaryInfo() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      createTableAndFlush(accumuloClient, table, true);
      String rFileName = getRFileName(accumuloClient, table);

      String output = execPrintInfo(rFileName, false);
      assertTrue(output.contains("RFile Version            : 8"));
      assertTrue(output.contains("Meta block     : accumulo.summaries.index"));
      assertFalse(output.contains("No summary data present in file"));

      output = execPrintInfo(rFileName, true);
      assertTrue(output.contains("RFile Version            : 8"));
      assertTrue(output.contains("Meta block     : accumulo.summaries.index"));
      assertFalse(output.contains("No summary data present in file"));
      assertTrue(output.contains("Summary data :"));
    }
  }

  // Test calling PrintInfo on a RFile from a previous Accumulo version. Verify that rather than
  // throwing an
  // exception (related to crypto params) a line indicating that crypto params are not read is
  // displayed instead.
  @Test
  public void testOldRFileVersion() throws Exception {
    String resource = "/org/apache/accumulo/test/ver_7.rf";
    File rFile = new File(tempDir, resource);
    FileUtils.copyURLToFile(requireNonNull(PrintInfoIT.class.getResource(resource)), rFile);
    String output = execPrintInfo(rFile.getAbsolutePath(), false);
    assertTrue(output.contains("Unable to read crypto params"));
    assertTrue(output.contains("RFile Version            : 7"));
    assertFalse(output.contains("No summary data present in file"));
  }

  // Create a table with data and create summary info if enableSummaries is true.
  private void createTableAndFlush(AccumuloClient client, final String tableName,
      final boolean enableSummaries) throws AccumuloException, TableExistsException,
      AccumuloSecurityException, TableNotFoundException {
    client.tableOperations().create(tableName);
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(0);
    try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
      Mutation m = new Mutation("row1");
      m.put("cf1", "cq1", new ColumnVisibility("A"), new Value("value1"));
      writer.addMutation(m);
      m = new Mutation("row2");
      m.put("cf2", "cq2", new ColumnVisibility("B"), new Value("value2"));
      writer.addMutation(m);
      m = new Mutation("row3");
      m.put("cf3", "cq3", new ColumnVisibility("A&B"), new Value("value3"));
      writer.addMutation(m);
    }
    if (enableSummaries) {
      SummarizerConfiguration sc1 =
          SummarizerConfiguration.builder(VisibilitySummarizer.class).build();
      client.tableOperations().addSummarizers(tableName, sc1);
    }
    client.tableOperations().flush(tableName, null, null, true);
  }

  // Get the name of the RFile associated with a table.
  private String getRFileName(final AccumuloClient client, final String tableName)
      throws Exception {
    boolean foundFile = false;
    String rfileName = null;
    try (BatchScanner bscanner =
        client.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1)) {
      String tableId = client.tableOperations().tableIdMap().get(tableName);
      bscanner.setRanges(
          Collections.singletonList(new Range(new Text(tableId + ";"), new Text(tableId + "<"))));
      bscanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      for (Map.Entry<Key,Value> entry : bscanner) {
        foundFile = true;
        rfileName = entry.getKey().getColumnQualifier().toString();
      }
      assertTrue(foundFile);
    }
    return rfileName;
  }

  // Execute PrintInfo on an RFile. Enable the --summary argument is useSummaryArg is true.
  private String execPrintInfo(final String rfile, final boolean useSummaryArg) throws Exception {
    String rfileOutput;
    MiniAccumuloClusterImpl.ProcessInfo p;
    if (useSummaryArg) {
      p = getCluster().exec(PrintInfo.class, rfile, "--summary");
    } else {
      p = getCluster().exec(PrintInfo.class, rfile);
    }
    assertEquals(0, p.getProcess().waitFor(), "Call to PrintInfo failed");
    rfileOutput = p.readStdOut();
    return rfileOutput;
  }

}
