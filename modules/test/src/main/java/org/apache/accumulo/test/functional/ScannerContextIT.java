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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.CoreMatchers;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ScannerContextIT extends AccumuloClusterHarness {

  private static final String CONTEXT = ScannerContextIT.class.getSimpleName();
  private static final String CONTEXT_PROPERTY = Property.VFS_CONTEXT_CLASSPATH_PROPERTY + CONTEXT;
  private static final String CONTEXT_DIR = "file://" + System.getProperty("user.dir") + "/target";
  private static final String CONTEXT_CLASSPATH = CONTEXT_DIR + "/Test.jar";
  private static int ITERATIONS = 10;
  private static final long WAIT = 7000;

  private FileSystem fs;

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Before
  public void checkCluster() throws Exception {
    Assume.assumeThat(getClusterType(), CoreMatchers.is(ClusterType.MINI));
    MiniAccumuloClusterImpl.class.cast(getCluster());
    fs = FileSystem.get(CachedConfiguration.getInstance());
  }

  private Path copyTestIteratorsJarToTmp() throws IOException {
    // Copy the test iterators jar to tmp
    Path baseDir = new Path(System.getProperty("user.dir"));
    Path targetDir = new Path(baseDir, "target");
    Path jarPath = new Path(targetDir, "TestJar-Iterators.jar");
    Path dstPath = new Path(CONTEXT_DIR + "/Test.jar");
    fs.copyFromLocalFile(jarPath, dstPath);
    // Sleep to ensure jar change gets picked up
    UtilWaitThread.sleep(WAIT);
    return dstPath;
  }

  @Test
  public void test() throws Exception {
    Path dstPath = copyTestIteratorsJarToTmp();
    try {
      Connector c = getConnector();
      // Set the classloader context property on the table to point to the test iterators jar file.
      c.instanceOperations().setProperty(CONTEXT_PROPERTY, CONTEXT_CLASSPATH);

      // Insert rows with the word "Test" in the value.
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (int i = 0; i < ITERATIONS; i++) {
        Mutation m = new Mutation("row" + i);
        m.put("cf", "col1", "Test");
        bw.addMutation(m);
      }
      bw.close();
      // Ensure that we can get the data back
      scanCheck(c, tableName, null, null, "Test");
      batchCheck(c, tableName, null, null, "Test");

      // This iterator is in the test iterators jar file
      IteratorSetting cfg = new IteratorSetting(21, "reverse", "org.apache.accumulo.test.functional.ValueReversingIterator");

      // Check that ValueReversingIterator is not already on the classpath by not setting the context. This should fail.
      try {
        scanCheck(c, tableName, cfg, null, "tseT");
        fail("This should have failed because context was not set");
      } catch (Exception e) {
        // Do nothing, this should fail as the classloader context is not set.
      }
      try {
        batchCheck(c, tableName, cfg, null, "tseT");
        fail("This should have failed because context was not set");
      } catch (Exception e) {
        // Do nothing, this should fail as the classloader context is not set.
      }

      // Ensure that the value is reversed using the iterator config and classloader context
      scanCheck(c, tableName, cfg, CONTEXT, "tseT");
      batchCheck(c, tableName, cfg, CONTEXT, "tseT");
    } finally {
      // Delete file in tmp
      fs.delete(dstPath, true);
    }
  }

  @Test
  public void testScanContextOverridesTableContext() throws Exception {
    Path dstPath = copyTestIteratorsJarToTmp();
    try {
      Connector c = getConnector();
      // Create two contexts FOO and ScanContextIT. The FOO context will point to a classpath
      // that contains nothing. The ScanContextIT context will point to the test iterators jar
      String tableContext = "FOO";
      String tableContextProperty = Property.VFS_CONTEXT_CLASSPATH_PROPERTY + tableContext;
      String tableContextDir = "file://" + System.getProperty("user.dir") + "/target";
      String tableContextClasspath = tableContextDir + "/TestFoo.jar";
      // Define both contexts
      c.instanceOperations().setProperty(tableContextProperty, tableContextClasspath);
      c.instanceOperations().setProperty(CONTEXT_PROPERTY, CONTEXT_CLASSPATH);

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      // Set the FOO context on the table
      c.tableOperations().setProperty(tableName, Property.TABLE_CLASSPATH.getKey(), tableContext);
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (int i = 0; i < ITERATIONS; i++) {
        Mutation m = new Mutation("row" + i);
        m.put("cf", "col1", "Test");
        bw.addMutation(m);
      }
      bw.close();
      scanCheck(c, tableName, null, null, "Test");
      batchCheck(c, tableName, null, null, "Test");
      // This iterator is in the test iterators jar file
      IteratorSetting cfg = new IteratorSetting(21, "reverse", "org.apache.accumulo.test.functional.ValueReversingIterator");

      // Check that ValueReversingIterator is not already on the classpath by not setting the context. This should fail.
      try {
        scanCheck(c, tableName, cfg, null, "tseT");
        fail("This should have failed because context was not set");
      } catch (Exception e) {
        // Do nothing, this should fail as the classloader context is not set.
      }
      try {
        batchCheck(c, tableName, cfg, null, "tseT");
        fail("This should have failed because context was not set");
      } catch (Exception e) {
        // Do nothing, this should fail as the classloader context is not set.
      }

      // Ensure that the value is reversed using the iterator config and classloader context
      scanCheck(c, tableName, cfg, CONTEXT, "tseT");
      batchCheck(c, tableName, cfg, CONTEXT, "tseT");
    } finally {
      // Delete file in tmp
      fs.delete(dstPath, true);
    }

  }

  @Test
  public void testOneScannerDoesntInterfereWithAnother() throws Exception {
    Path dstPath = copyTestIteratorsJarToTmp();
    try {
      Connector c = getConnector();
      // Set the classloader context property on the table to point to the test iterators jar file.
      c.instanceOperations().setProperty(CONTEXT_PROPERTY, CONTEXT_CLASSPATH);

      // Insert rows with the word "Test" in the value.
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (int i = 0; i < ITERATIONS; i++) {
        Mutation m = new Mutation("row" + i);
        m.put("cf", "col1", "Test");
        bw.addMutation(m);
      }
      bw.close();

      Scanner one = c.createScanner(tableName, Authorizations.EMPTY);

      Scanner two = c.createScanner(tableName, Authorizations.EMPTY);

      IteratorSetting cfg = new IteratorSetting(21, "reverse", "org.apache.accumulo.test.functional.ValueReversingIterator");
      one.addScanIterator(cfg);
      one.setClassLoaderContext(CONTEXT);

      Iterator<Entry<Key,Value>> iterator = one.iterator();
      for (int i = 0; i < ITERATIONS; i++) {
        assertTrue(iterator.hasNext());
        Entry<Key,Value> next = iterator.next();
        assertEquals("tseT", next.getValue().toString());
      }

      Iterator<Entry<Key,Value>> iterator2 = two.iterator();
      for (int i = 0; i < ITERATIONS; i++) {
        assertTrue(iterator2.hasNext());
        Entry<Key,Value> next = iterator2.next();
        assertEquals("Test", next.getValue().toString());
      }

    } finally {
      // Delete file in tmp
      fs.delete(dstPath, true);
    }
  }

  @Test
  public void testClearContext() throws Exception {
    Path dstPath = copyTestIteratorsJarToTmp();
    try {
      Connector c = getConnector();
      // Set the classloader context property on the table to point to the test iterators jar file.
      c.instanceOperations().setProperty(CONTEXT_PROPERTY, CONTEXT_CLASSPATH);

      // Insert rows with the word "Test" in the value.
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (int i = 0; i < ITERATIONS; i++) {
        Mutation m = new Mutation("row" + i);
        m.put("cf", "col1", "Test");
        bw.addMutation(m);
      }
      bw.close();

      Scanner one = c.createScanner(tableName, Authorizations.EMPTY);
      IteratorSetting cfg = new IteratorSetting(21, "reverse", "org.apache.accumulo.test.functional.ValueReversingIterator");
      one.addScanIterator(cfg);
      one.setClassLoaderContext(CONTEXT);

      Iterator<Entry<Key,Value>> iterator = one.iterator();
      for (int i = 0; i < ITERATIONS; i++) {
        assertTrue(iterator.hasNext());
        Entry<Key,Value> next = iterator.next();
        assertEquals("tseT", next.getValue().toString());
      }

      one.removeScanIterator("reverse");
      one.clearClassLoaderContext();
      iterator = one.iterator();
      for (int i = 0; i < ITERATIONS; i++) {
        assertTrue(iterator.hasNext());
        Entry<Key,Value> next = iterator.next();
        assertEquals("Test", next.getValue().toString());
      }

    } finally {
      // Delete file in tmp
      fs.delete(dstPath, true);
    }
  }

  private void scanCheck(Connector c, String tableName, IteratorSetting cfg, String context, String expected) throws Exception {
    Scanner bs = c.createScanner(tableName, Authorizations.EMPTY);
    if (null != context) {
      bs.setClassLoaderContext(context);
    }
    if (null != cfg) {
      bs.addScanIterator(cfg);
    }
    Iterator<Entry<Key,Value>> iterator = bs.iterator();
    for (int i = 0; i < ITERATIONS; i++) {
      assertTrue(iterator.hasNext());
      Entry<Key,Value> next = iterator.next();
      assertEquals(expected, next.getValue().toString());
    }
    assertFalse(iterator.hasNext());
  }

  private void batchCheck(Connector c, String tableName, IteratorSetting cfg, String context, String expected) throws Exception {
    BatchScanner bs = c.createBatchScanner(tableName, Authorizations.EMPTY, 1);
    bs.setRanges(Collections.singleton(new Range()));
    try {
      if (null != context) {
        bs.setClassLoaderContext(context);
      }
      if (null != cfg) {
        bs.addScanIterator(cfg);
      }
      Iterator<Entry<Key,Value>> iterator = bs.iterator();
      for (int i = 0; i < ITERATIONS; i++) {
        assertTrue(iterator.hasNext());
        Entry<Key,Value> next = iterator.next();
        assertEquals(expected, next.getValue().toString());
      }
      assertFalse(iterator.hasNext());
    } finally {
      bs.close();
    }
  }

}
