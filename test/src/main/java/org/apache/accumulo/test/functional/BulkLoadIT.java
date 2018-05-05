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
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Tests new bulk import technique. If the old technique ever gets removed this will replace
 * {@link BulkFileIT}
 *
 * @since 2.0
 */
public class BulkLoadIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setMemory(ServerType.TABLET_SERVER, 128 * 4, MemoryUnit.MEGABYTE);

    // use raw local file system
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void testSingleTabletSingleFile() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    addSplits(tableName, "0333");

    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    FileSystem fs = getCluster().getFileSystem();
    String rootPath = cluster.getTemporaryPath().toString();

    String dir = rootPath + "/testSingleTabletSingleFileNoSplits-" + getUniqueNames(1)[0];
    Path bulkDir = new Path(dir);

    fs.delete(bulkDir, true);

    FileSKVWriter writer1 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f1." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 332);
    writer1.close();

    c.tableOperations().addFilesTo(tableName).from(dir).load();
  }

  @Test
  public void testSingleTabletSingleFileNoSplits() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    FileSystem fs = getCluster().getFileSystem();
    String rootPath = cluster.getTemporaryPath().toString();

    String dir = rootPath + "/testSingleTabletSingleFileNoSplits-" + getUniqueNames(1)[0];
    Path bulkDir = new Path(dir);

    fs.delete(bulkDir, true);

    FileSKVWriter writer1 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f1." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 333);
    writer1.close();

    c.tableOperations().addFilesTo(tableName).from(dir).load();
  }

  @Test
  public void testBadPermissions() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    addSplits(tableName, "0333");

    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    FileSystem fs = getCluster().getFileSystem();
    String rootPath = cluster.getTemporaryPath().toString();

    String dir = rootPath + "/testBadPermissions-" + getUniqueNames(1)[0];
    Path bulkDir = new Path(dir);

    fs.delete(bulkDir, true);

    FileSKVWriter writer1 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f1." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 333);
    writer1.close();

    Path rFilePath = new Path(dir, "f1." + RFile.EXTENSION);
    FsPermission originalPerms = fs.getFileStatus(rFilePath).getPermission();
    fs.setPermission(rFilePath, FsPermission.valueOf("----------"));
    try {
      c.tableOperations().addFilesTo(tableName).from(dir).load();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (!(cause instanceof FileNotFoundException)
          && !(cause.getCause() instanceof FileNotFoundException))
        fail("Expected FileNotFoundException but threw " + e.getCause());
    } finally {
      fs.setPermission(rFilePath, originalPerms);
    }

    originalPerms = fs.getFileStatus(bulkDir).getPermission();
    fs.setPermission(bulkDir, FsPermission.valueOf("dr--r--r--"));
    try {
      c.tableOperations().addFilesTo(tableName).from(dir).load();
    } catch (AccumuloException ae) {
      if (!(ae.getCause() instanceof FileNotFoundException))
        fail("Expected FileNotFoundException but threw " + ae.getCause());
    } finally {
      fs.setPermission(bulkDir, originalPerms);
    }
  }

  @Test
  public void testBulkFile() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    addSplits(tableName, "0333 0666 0999 1333 1666");

    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    FileSystem fs = getCluster().getFileSystem();

    String rootPath = cluster.getTemporaryPath().toString();

    String dir = rootPath + "/testBulkFile-" + getUniqueNames(1)[0];

    fs.delete(new Path(dir), true);

    // TODO verify that data gets loaded in appropriate Tablets
    // 1 Tablet 0333-null
    FileSKVWriter writer1 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f1." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 333);
    writer1.close();

    // 2 Tablets 0666-0334, 0999-0667
    FileSKVWriter writer2 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f2." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer2.startDefaultLocalityGroup();
    writeData(writer2, 334, 999);
    writer2.close();

    // 2 Tablets 1333-1000, 1666-1334
    FileSKVWriter writer3 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f3." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer3.startDefaultLocalityGroup();
    // writeData(writer3, 1000, 1999);
    writeData(writer3, 1000, 1499);
    writer3.close();

    // 2 Tablets 1666-1334, >1666
    FileSKVWriter writer4 = FileOperations.getInstance().newWriterBuilder()
        .forFile(dir + "/f4." + RFile.EXTENSION, fs, conf).withTableConfiguration(aconf).build();
    writer4.startDefaultLocalityGroup();
    writeData(writer4, 1500, 1999);
    writer4.close();

    // TODO test c.tableOperations().offline(tableName, true);
    c.tableOperations().addFilesTo(tableName).from(dir).load();

    verifyData(tableName, 0, 1999);
  }

  private void addSplits(String tableName, String splitString) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (String split : splitString.split(" "))
      splits.add(new Text(split));
    getConnector().tableOperations().addSplits(tableName, splits);
  }

  private void verifyData(String table, int s, int e) throws Exception {
    try (Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY)) {

      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      for (int i = s; i <= e; i++) {
        if (!iter.hasNext())
          throw new Exception("row " + i + " not found");

        Entry<Key,Value> entry = iter.next();

        String row = String.format("%04d", i);

        if (!entry.getKey().getRow().equals(new Text(row)))
          throw new Exception("unexpected row " + entry.getKey() + " " + i);

        if (Integer.parseInt(entry.getValue().toString()) != i)
          throw new Exception("unexpected value " + entry + " " + i);
      }

      if (iter.hasNext())
        throw new Exception("found more than expected " + iter.next());
    }
  }

  private void writeData(FileSKVWriter w, int s, int e) throws Exception {
    for (int i = s; i <= e; i++) {
      w.append(new Key(new Text(String.format("%04d", i))),
          new Value(Integer.toString(i).getBytes(UTF_8)));
    }
  }

}
