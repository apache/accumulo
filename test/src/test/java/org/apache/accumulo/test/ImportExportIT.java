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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterIT;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * ImportTable didn't correctly place absolute paths in metadata. This resulted in the imported table only being usable when the actual HDFS directory for
 * Accumulo was the same as Property.INSTANCE_DFS_DIR. If any other HDFS directory was used, any interactions with the table would fail because the relative
 * path in the metadata table (created by the ImportTable process) would be converted to a non-existent absolute path.
 * <p>
 * ACCUMULO-3215
 *
 */
// TODO Can switch to AccumuloClusterIT when FileSystem access from the Cluster is implemented.
public class ImportExportIT extends SharedMiniClusterIT {

  private static final Logger log = LoggerFactory.getLogger(ImportExportIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void testExportImportThenScan() throws Exception {
    Connector conn = getConnector();

    String[] tableNames = getUniqueNames(2);
    String srcTable = tableNames[0], destTable = tableNames[1];
    conn.tableOperations().create(srcTable);

    BatchWriter bw = conn.createBatchWriter(srcTable, new BatchWriterConfig());
    for (int row = 0; row < 1000; row++) {
      Mutation m = new Mutation(Integer.toString(row));
      for (int col = 0; col < 100; col++) {
        m.put(Integer.toString(col), "", Integer.toString(col * 2));
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().compact(srcTable, null, null, true, true);

    // Make a directory we can use to throw the export and import directories
    // Must exist on the filesystem the cluster is running.
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests", getClass().getName());
    baseDir.mkdirs();
    File exportDir = new File(baseDir, "export");
    FileUtils.deleteQuietly(exportDir);
    exportDir.mkdir();
    File importDir = new File(baseDir, "import");
    FileUtils.deleteQuietly(importDir);
    importDir.mkdir();

    log.info("Exporting table to {}", exportDir.getAbsolutePath());
    log.info("Importing table from {}", importDir.getAbsolutePath());

    // Offline the table
    conn.tableOperations().offline(srcTable, true);
    // Then export it
    conn.tableOperations().exportTable(srcTable, exportDir.getAbsolutePath());

    // Make sure the distcp.txt file that exporttable creates is available
    File distcp = new File(exportDir, "distcp.txt");
    Assert.assertTrue("Distcp file doesn't exist", distcp.exists());
    FileInputStream fis = new FileInputStream(distcp);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

    // Copy each file that was exported to the import directory
    String line;
    while (null != (line = reader.readLine())) {
      File f = new File(line.substring(5));
      Assert.assertTrue("File doesn't exist: " + f, f.exists());
      File dest = new File(importDir, f.getName());
      Assert.assertFalse("Did not expect " + dest + " to exist", dest.exists());
      Files.copy(f, dest);
    }

    reader.close();

    log.info("Import dir: {}", Arrays.toString(importDir.list()));

    // Import the exported data into a new table
    conn.tableOperations().importTable(destTable, importDir.getAbsolutePath());

    // Get the table ID for the table that the importtable command created
    final String tableId = conn.tableOperations().tableIdMap().get(destTable);
    Assert.assertNotNull(tableId);

    // Get all `file` colfams from the metadata table for the new table
    log.info("Imported into table with ID: {}", tableId);
    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
    s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(s);

    // Should find a single entry
    for (Entry<Key,Value> fileEntry : s) {
      Key k = fileEntry.getKey();
      String value = fileEntry.getValue().toString();
      if (k.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        // The file should be an absolute URI (file:///...), not a relative path (/b-000.../I000001.rf)
        String fileUri = k.getColumnQualifier().toString();
        Assert.assertFalse("Imported files should have absolute URIs, not relative: " + fileUri, looksLikeRelativePath(fileUri));
      } else if (k.getColumnFamily().equals(MetadataSchema.TabletsSection.ServerColumnFamily.NAME)) {
        Assert.assertFalse("Server directory should have absolute URI, not relative: " + value, looksLikeRelativePath(value));
      } else {
        Assert.fail("Got expected pair: " + k + "=" + fileEntry.getValue());
      }
    }

    // Online the original table before we verify equivalence
    conn.tableOperations().online(srcTable, true);

    verifyTableEquality(conn, srcTable, destTable);
  }

  private void verifyTableEquality(Connector conn, String srcTable, String destTable) throws Exception {
    Iterator<Entry<Key,Value>> src = conn.createScanner(srcTable, Authorizations.EMPTY).iterator(), dest = conn.createScanner(destTable, Authorizations.EMPTY)
        .iterator();
    Assert.assertTrue("Could not read any data from source table", src.hasNext());
    Assert.assertTrue("Could not read any data from destination table", dest.hasNext());
    while (src.hasNext() && dest.hasNext()) {
      Entry<Key,Value> orig = src.next(), copy = dest.next();
      Assert.assertEquals(orig.getKey(), copy.getKey());
      Assert.assertEquals(orig.getValue(), copy.getValue());
    }
    Assert.assertFalse("Source table had more data to read", src.hasNext());
    Assert.assertFalse("Dest table had more data to read", dest.hasNext());
  }

  private boolean looksLikeRelativePath(String uri) {
    if (uri.startsWith("/" + Constants.BULK_PREFIX)) {
      if ('/' == uri.charAt(10)) {
        return true;
      }
    } else if (uri.startsWith("/" + Constants.CLONE_PREFIX)) {
      return true;
    }

    return false;
  }
}
