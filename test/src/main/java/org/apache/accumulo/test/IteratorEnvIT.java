package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test that objects in IteratorEnvironment returned from the server are as expected.
 */
public class IteratorEnvIT extends AccumuloClusterHarness {
  @ClassRule
  public static ExpectedException exception = ExpectedException.none();

  private AccumuloClient client;

  public static class ScanIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.scan;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                     IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(env, scope);
    }
  }

  public static class MajcIter extends WrappingIterator {
    IteratorScope scope = IteratorScope.majc;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                     IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      testEnv(env, scope);
    }
  }

  /**
   * Checking for compaction on a scan should throw an error.
   */
  public static class BadStateIter extends WrappingIterator {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                     IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      try {
        assertFalse(env.isUserCompaction());
        fail("Expected to throw IllegalStateException when checking compaction on a scan.");
      } catch (IllegalStateException e){};
    }
  }

  /**
   * Test the environment methods return what is expected.
   */
  public static void testEnv(IteratorEnvironment env, IteratorScope scope) {
    //env.getConfig();
    //env.getServiceEnv();
    assertEquals(scope, env.getIteratorScope());
    assertFalse(env.isSamplingEnabled());
    if (scope != IteratorScope.scan) {
      assertFalse(env.isUserCompaction());
      assertFalse(env.isFullMajorCompaction());
    }
  }

  @Before
  public void setup() {
    client = createAccumuloClient();
  }

  @After
  public void finish() { client.close(); }

  /**
   * Checking for compaction on a scan should throw an error.
   */
  @Test
  public void testBadState() throws Exception {
    testScan(BadStateIter.class);
  }

  /**
   * Test the scan environment methods return what is expected.
   */
  @Test
  public void testScanEnv() throws Exception {
    testScan(ScanIter.class);
  }

  private void testScan(Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass)
      throws Exception {
    String tableName = getUniqueNames(1)[0];
    client.tableOperations().create(tableName);

    writeData(tableName);

    IteratorSetting cfg = new IteratorSetting(1, iteratorClass);
    try (Scanner scan = client.createScanner(tableName)) {
      scan.addScanIterator(cfg);
      Iterator<Map.Entry<Key,Value>> iter = scan.iterator();
      iter.forEachRemaining(e ->
          assertEquals("cf1", e.getKey().getColumnFamily().toString()));
    }
  }

  private void writeData(String tableName) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("row1");
      m.at().family("cf1").qualifier("cq1").put("val1");
      bw.addMutation(m);
      m = new Mutation("row2");
      m.at().family("cf1").qualifier("cq1").put("val2");
      bw.addMutation(m);
      m = new Mutation("row3");
      m.at().family("cf1").qualifier("cq1").put("val3");
      bw.addMutation(m);
    }
  }
}
