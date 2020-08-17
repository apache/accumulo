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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MasterBulkFailureIT extends AccumuloClusterHarness {
  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws TableExistsException, AccumuloSecurityException, AccumuloException,
      TableNotFoundException, IOException {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1");
    c.instanceOperations().setProperty(Property.TSERV_BULK_MAX_TABLET_OVERLAP.getKey(), "2");
    // splits
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 50; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
    c.tableOperations().addSplits(tableName, splits);

    // create a bulk load file
    final FileSystem fs = getCluster().getFileSystem();
    final Path basePath = getCluster().getTemporaryPath();
    CachedConfiguration.setInstance(fs.getConf());

    final Path base = new Path(basePath, "testBulkLoad" + tableName);
    fs.delete(base, true);
    fs.mkdirs(base);

    Path bulkFailures = new Path(base, "failures");
    Path files = new Path(base, "files");
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);
    FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(files.toString() + "/bulk." + RFile.EXTENSION, fs, fs.getConf())
        .withTableConfiguration(AccumuloConfiguration.getDefaultConfiguration()).build();
    writer.startDefaultLocalityGroup();
    for (int j = 0; j < 5; j++) {
      writer.append(new Key(Integer.toString(j)), new Value(new byte[0]));
    }
    writer.close();

    Path file = new Path(files, "bulk." + RFile.EXTENSION);

    c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(),
        false);

    RemoteIterator<LocatedFileStatus> status = fs.listFiles(bulkFailures, false);
    assertTrue(status.hasNext());
    Path newPath = status.next().getPath();
    assertFalse(status.hasNext());

    // verify nothing loaded
    Scanner s = c.createScanner(tableName, new Authorizations());
    s.setRange(new Range());
    Iterator<Map.Entry<Key,Value>> iterator = s.iterator();
    assertFalse(iterator.hasNext());

    // move the file back to its original location, reset the property higher and try again
    fs.rename(newPath, file);

    c.instanceOperations().setProperty(Property.TSERV_BULK_MAX_TABLET_OVERLAP.getKey(), "4");

    c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(),
        false);

    status = fs.listFiles(bulkFailures, false);
    assertFalse(status.hasNext());

    // verify the keys were loaded
    s = c.createScanner(tableName, new Authorizations());
    s.setRange(new Range());
    iterator = s.iterator();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(iterator.hasNext());
  }
}
