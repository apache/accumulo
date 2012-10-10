/**
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
package org.apache.accumulo.core.client.mock;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class MockTableOperationsTest {
  @SuppressWarnings("deprecation")
  @Test
  public void testTableNotFound() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    Instance instance = new MockInstance("topstest");
    Connector conn = instance.getConnector("user", "pass");
    String t = "tableName";
    try {
      conn.tableOperations().addAggregators(t, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().attachIterator(t, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().checkIteratorConflicts(t, null, EnumSet.allOf(IteratorScope.class));
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().delete(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getIteratorSetting(t, null, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getProperties(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getSplits(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().listIterators(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().removeIterator(t, null, null);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().rename(t, t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    conn.tableOperations().create(t);
    try {
      conn.tableOperations().create(t);
      Assert.fail();
    } catch (TableExistsException e) {}
    try {
      conn.tableOperations().rename(t, t);
      Assert.fail();
    } catch (TableExistsException e) {}
  }
  
  private static class ImportTestFilesAndData {
    Path importPath;
    Path failurePath;
    List<Pair<Key, Value>> keyVals;
  }

  @Test
  public void testImport() throws Throwable {
    ImportTestFilesAndData dataAndFiles = prepareTestFiles();
    Instance instance = new MockInstance("foo");
    Connector connector = instance.getConnector(new AuthInfo("user", ByteBuffer
        .wrap(new byte[0]), "foo"));
    TableOperations tableOperations = connector.tableOperations();
    tableOperations.create("a_table");
    tableOperations.importDirectory("a_table",
        dataAndFiles.importPath.toString(),
        dataAndFiles.failurePath.toString(), false);
    Scanner scanner = connector.createScanner("a_table", new Authorizations());
    Iterator<Entry<Key, Value>> iterator = scanner.iterator();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(iterator.hasNext());
      Entry<Key, Value> kv = iterator.next();
      Pair<Key, Value> expected = dataAndFiles.keyVals.get(i);
      Assert.assertEquals(expected.getFirst(), kv.getKey());
      Assert.assertEquals(expected.getSecond(), kv.getValue());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  private ImportTestFilesAndData prepareTestFiles() throws Throwable {
    Configuration defaultConf = new Configuration();
    Path tempFile = new Path("target/accumulo-test/import/sample.map");
    Path failures = new Path("target/accumulo-test/failures/");
    FileSystem fs = FileSystem.get(new URI("file:///"), defaultConf);
    fs.deleteOnExit(tempFile);
    fs.deleteOnExit(failures);
    fs.delete(failures, true);
    fs.mkdirs(failures);
    fs.mkdirs(tempFile.getParent());
    FileSKVWriter writer = FileOperations.getInstance().openWriter(
        tempFile.toString(), fs, defaultConf,
        AccumuloConfiguration.getDefaultConfiguration());
    List<Pair<Key, Value>> keyVals = new ArrayList<Pair<Key, Value>>();
    for (int i = 0; i < 5; i++) {
      keyVals.add(new Pair<Key, Value>(new Key("a" + i, "b" + i, "c" + i,
          new ColumnVisibility(""), 1000l + i), new Value(Integer.toString(i)
          .getBytes())));
    }
    for (Pair<Key, Value> keyVal : keyVals) {
      writer.append(keyVal.getFirst(), keyVal.getSecond());
    }
    writer.close();
    ImportTestFilesAndData files = new ImportTestFilesAndData();
    files.failurePath = failures;
    files.importPath = tempFile.getParent();
    files.keyVals = keyVals;
    return files;
  }

  @Test(expected = TableNotFoundException.class)
  public void testFailsWithNoTable() throws Throwable {
    Instance instance = new MockInstance("foo");
    Connector connector = instance.getConnector(new AuthInfo("user", ByteBuffer
        .wrap(new byte[0]), "foo"));
    TableOperations tableOperations = connector.tableOperations();
    ImportTestFilesAndData testFiles = prepareTestFiles();
    tableOperations.importDirectory("doesnt_exist_table",
        testFiles.importPath.toString(), testFiles.failurePath.toString(),
        false);
  }

  @Test(expected = IOException.class)
  public void testFailsWithNonEmptyFailureDirectory() throws Throwable {
    Instance instance = new MockInstance("foo");
    Connector connector = instance.getConnector(new AuthInfo("user", ByteBuffer
        .wrap(new byte[0]), "foo"));
    TableOperations tableOperations = connector.tableOperations();
    ImportTestFilesAndData testFiles = prepareTestFiles();
    FileSystem fs = testFiles.failurePath.getFileSystem(new Configuration());
    fs.open(testFiles.failurePath.suffix("/something")).close();
    tableOperations.importDirectory("doesnt_exist_table",
        testFiles.importPath.toString(), testFiles.failurePath.toString(),
        false);
  }
  
}
