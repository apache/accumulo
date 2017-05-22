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
package org.apache.accumulo.core.client.mock;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Iterators;

@Deprecated
public class MockTableOperationsTest {

  @Rule
  public TestName test = new TestName();

  private Connector conn;

  @Before
  public void setupInstance() throws Exception {
    Instance inst = new MockInstance(test.getMethodName());
    conn = inst.getConnector("user", new PasswordToken("pass"));
  }

  @Test
  public void testCreateUseVersions() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    String t = "tableName1";

    {
      conn.tableOperations().create(t, new NewTableConfiguration().withoutDefaultIterators().setTimeType(TimeType.LOGICAL));

      writeVersionable(conn, t, 3);
      assertVersionable(conn, t, 3);

      IteratorSetting settings = new IteratorSetting(20, VersioningIterator.class);
      conn.tableOperations().attachIterator(t, settings);

      assertVersionable(conn, t, 1);

      conn.tableOperations().delete(t);
    }

    {
      conn.tableOperations().create(t, new NewTableConfiguration().setTimeType(TimeType.MILLIS));

      try {
        IteratorSetting settings = new IteratorSetting(20, VersioningIterator.class);
        conn.tableOperations().attachIterator(t, settings);
        Assert.fail();
      } catch (AccumuloException ex) {}

      writeVersionable(conn, t, 3);
      assertVersionable(conn, t, 1);

      conn.tableOperations().delete(t);
    }
  }

  protected void writeVersionable(Connector c, String tableName, int size) throws TableNotFoundException, MutationsRejectedException {
    for (int i = 0; i < size; i++) {
      BatchWriter w = c.createBatchWriter(tableName, new BatchWriterConfig());
      Mutation m = new Mutation("row1");
      m.put("cf", "cq", String.valueOf(i));
      w.addMutation(m);
      w.close();
    }
  }

  protected void assertVersionable(Connector c, String tableName, int size) throws TableNotFoundException {
    BatchScanner s = c.createBatchScanner(tableName, Authorizations.EMPTY, 1);
    s.setRanges(Collections.singleton(Range.exact("row1", "cf", "cq")));
    int count = 0;
    for (Map.Entry<Key,Value> e : s) {
      Assert.assertEquals("row1", e.getKey().getRow().toString());
      Assert.assertEquals("cf", e.getKey().getColumnFamily().toString());
      Assert.assertEquals("cq", e.getKey().getColumnQualifier().toString());
      count++;

    }
    Assert.assertEquals(size, count);
    s.close();
  }

  @Test
  public void testTableNotFound() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    IteratorSetting setting = new IteratorSetting(100, "myvers", VersioningIterator.class);
    String t = "tableName";
    try {
      conn.tableOperations().attachIterator(t, setting);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().checkIteratorConflicts(t, setting, EnumSet.allOf(IteratorScope.class));
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().delete(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getIteratorSetting(t, "myvers", IteratorScope.scan);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().getProperties(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().listSplits(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().listIterators(t);
      Assert.fail();
    } catch (TableNotFoundException e) {}
    try {
      conn.tableOperations().removeIterator(t, null, EnumSet.noneOf(IteratorScope.class));
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
    List<Pair<Key,Value>> keyVals;
  }

  @Test
  public void testImport() throws Throwable {
    ImportTestFilesAndData dataAndFiles = prepareTestFiles();
    TableOperations tableOperations = conn.tableOperations();
    tableOperations.create("a_table");
    tableOperations.importDirectory("a_table", dataAndFiles.importPath.toString(), dataAndFiles.failurePath.toString(), false);
    Scanner scanner = conn.createScanner("a_table", new Authorizations());
    Iterator<Entry<Key,Value>> iterator = scanner.iterator();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(iterator.hasNext());
      Entry<Key,Value> kv = iterator.next();
      Pair<Key,Value> expected = dataAndFiles.keyVals.get(i);
      Assert.assertEquals(expected.getFirst(), kv.getKey());
      Assert.assertEquals(expected.getSecond(), kv.getValue());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  private ImportTestFilesAndData prepareTestFiles() throws Throwable {
    Configuration defaultConf = new Configuration();
    Path tempFile = new Path("target/accumulo-test/import/sample.rf");
    Path failures = new Path("target/accumulo-test/failures/");
    FileSystem fs = FileSystem.get(new URI("file:///"), defaultConf);
    fs.deleteOnExit(tempFile);
    fs.deleteOnExit(failures);
    fs.delete(failures, true);
    fs.delete(tempFile, true);
    fs.mkdirs(failures);
    fs.mkdirs(tempFile.getParent());
    FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder().forFile(tempFile.toString(), fs, defaultConf)
        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
    writer.startDefaultLocalityGroup();
    List<Pair<Key,Value>> keyVals = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      keyVals.add(new Pair<>(new Key("a" + i, "b" + i, "c" + i, new ColumnVisibility(""), 1000l + i), new Value(Integer.toString(i).getBytes())));
    }
    for (Pair<Key,Value> keyVal : keyVals) {
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
    TableOperations tableOperations = conn.tableOperations();
    ImportTestFilesAndData testFiles = prepareTestFiles();
    tableOperations.importDirectory("doesnt_exist_table", testFiles.importPath.toString(), testFiles.failurePath.toString(), false);
  }

  @Test(expected = IOException.class)
  public void testFailsWithNonEmptyFailureDirectory() throws Throwable {
    TableOperations tableOperations = conn.tableOperations();
    ImportTestFilesAndData testFiles = prepareTestFiles();
    FileSystem fs = testFiles.failurePath.getFileSystem(new Configuration());
    fs.open(testFiles.failurePath.suffix("/something")).close();
    tableOperations.importDirectory("doesnt_exist_table", testFiles.importPath.toString(), testFiles.failurePath.toString(), false);
  }

  @Test
  public void testDeleteRows() throws Exception {
    TableOperations to = conn.tableOperations();
    to.create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
    for (int r = 0; r < 20; r++) {
      Mutation m = new Mutation("" + r);
      for (int c = 0; c < 5; c++) {
        m.put(new Text("cf"), new Text("" + c), new Value(("" + c).getBytes()));
      }
      bw.addMutation(m);
    }
    bw.flush();
    to.deleteRows("test", new Text("1"), new Text("2"));
    Scanner s = conn.createScanner("test", Authorizations.EMPTY);
    int oneCnt = 0;
    for (Entry<Key,Value> entry : s) {
      char rowStart = entry.getKey().getRow().toString().charAt(0);
      Assert.assertTrue(rowStart != '2');
      oneCnt += rowStart == '1' ? 1 : 0;
    }
    Assert.assertEquals(5, oneCnt);
  }

  @Test
  public void testDeleteRowsWithNullKeys() throws Exception {
    TableOperations to = conn.tableOperations();
    to.create("test2");
    BatchWriter bw = conn.createBatchWriter("test2", new BatchWriterConfig());
    for (int r = 0; r < 30; r++) {
      Mutation m = new Mutation(Integer.toString(r));
      for (int c = 0; c < 5; c++) {
        m.put(new Text("cf"), new Text(Integer.toString(c)), new Value(Integer.toString(c).getBytes()));
      }
      bw.addMutation(m);
    }
    bw.flush();

    // test null end
    // will remove rows 4 through 9 (6 * 5 = 30 entries)
    to.deleteRows("test2", new Text("30"), null);
    Scanner s = conn.createScanner("test2", Authorizations.EMPTY);
    int rowCnt = 0;
    for (Entry<Key,Value> entry : s) {
      String rowId = entry.getKey().getRow().toString();
      Assert.assertFalse(rowId.startsWith("30"));
      rowCnt++;
    }
    s.close();
    Assert.assertEquals(120, rowCnt);

    // test null start
    // will remove 0-1, 10-19, 2
    to.deleteRows("test2", null, new Text("2"));
    s = conn.createScanner("test2", Authorizations.EMPTY);
    rowCnt = 0;
    for (Entry<Key,Value> entry : s) {
      char rowStart = entry.getKey().getRow().toString().charAt(0);
      Assert.assertTrue(rowStart >= '2');
      rowCnt++;
    }
    s.close();
    Assert.assertEquals(55, rowCnt);

    // test null start and end
    // deletes everything still left
    to.deleteRows("test2", null, null);
    s = conn.createScanner("test2", Authorizations.EMPTY);
    rowCnt = Iterators.size(s.iterator());
    s.close();
    to.delete("test2");
    Assert.assertEquals(0, rowCnt);

  }

  @Test
  public void testTableIdMap() throws Exception {
    TableOperations tops = conn.tableOperations();
    tops.create("foo");

    // Should get a table ID, not the table name
    Assert.assertNotEquals("foo", tops.tableIdMap().get("foo"));
  }
}
