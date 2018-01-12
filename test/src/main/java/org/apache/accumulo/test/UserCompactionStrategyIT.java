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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.ConfigurableCompactionIT;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class UserCompactionStrategyIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 3 * 60;
  }

  @After
  public void checkForDanglingFateLocks() {
    FunctionalTestUtils.assertNoDanglingFateLocks(getConnector().getInstance(), getCluster());
  }

  @Test
  public void testDropA() throws Exception {
    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    writeFlush(c, tableName, "a");
    writeFlush(c, tableName, "b");
    // create a file that starts with A containing rows 'a' and 'b'
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    writeFlush(c, tableName, "c");
    writeFlush(c, tableName, "d");

    // drop files that start with A
    CompactionStrategyConfig csConfig = new CompactionStrategyConfig(TestCompactionStrategy.class.getName());
    csConfig.setOptions(ImmutableMap.of("dropPrefix", "A", "inputPrefix", "F"));
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig));

    Assert.assertEquals(ImmutableSet.of("c", "d"), getRows(c, tableName));

    // this compaction should not drop files starting with A
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    Assert.assertEquals(ImmutableSet.of("c", "d"), getRows(c, tableName));
  }

  private void testDropNone(Map<String,String> options) throws Exception {

    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    writeFlush(c, tableName, "a");
    writeFlush(c, tableName, "b");

    CompactionStrategyConfig csConfig = new CompactionStrategyConfig(TestCompactionStrategy.class.getName());
    csConfig.setOptions(options);
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig));

    Assert.assertEquals(ImmutableSet.of("a", "b"), getRows(c, tableName));
  }

  @Test
  public void testDropNone() throws Exception {
    // test a compaction strategy that selects no files. In this case there is no work to do, want to ensure it does not hang.

    testDropNone(ImmutableMap.of("inputPrefix", "Z"));
  }

  @Test
  public void testDropNone2() throws Exception {
    // test a compaction strategy that selects no files. This differs testDropNone() in that shouldCompact() will return true and getCompactionPlan() will
    // return no work to do.

    testDropNone(ImmutableMap.of("inputPrefix", "Z", "shouldCompact", "true"));
  }

  @Test
  public void testPerTableClasspath() throws Exception {
    // Can't assume that a test-resource will be on the server's classpath
    Assume.assumeTrue(ClusterType.MINI == getClusterType());

    // test per-table classpath + user specified compaction strategy

    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    File target = new File(System.getProperty("user.dir"), "target");
    Assert.assertTrue(target.mkdirs() || target.isDirectory());
    File destFile = installJar(target, "/TestCompactionStrat.jar");
    c.tableOperations().create(tableName);
    c.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "context1", destFile.toString());
    c.tableOperations().setProperty(tableName, Property.TABLE_CLASSPATH.getKey(), "context1");

    c.tableOperations().addSplits(tableName, new TreeSet<>(Arrays.asList(new Text("efg"))));

    writeFlush(c, tableName, "a");
    writeFlush(c, tableName, "b");

    writeFlush(c, tableName, "h");
    writeFlush(c, tableName, "i");

    Assert.assertEquals(4, FunctionalTestUtils.countRFiles(c, tableName));

    // EfgCompactionStrat will only compact a tablet w/ end row of 'efg'. No other tablets are compacted.
    CompactionStrategyConfig csConfig = new CompactionStrategyConfig("org.apache.accumulo.test.EfgCompactionStrat");
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig));

    Assert.assertEquals(3, FunctionalTestUtils.countRFiles(c, tableName));

    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    Assert.assertEquals(2, FunctionalTestUtils.countRFiles(c, tableName));
  }

  private static File installJar(File destDir, String jarFile) throws IOException {
    File destName = new File(destDir, new File(jarFile).getName());
    FileUtils.copyInputStreamToFile(ConfigurableCompactionIT.class.getResourceAsStream(jarFile), destName);
    return destName;
  }

  @Test
  public void testIterators() throws Exception {
    // test compaction strategy + iterators

    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    writeFlush(c, tableName, "a");
    writeFlush(c, tableName, "b");
    // create a file that starts with A containing rows 'a' and 'b'
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    writeFlush(c, tableName, "c");
    writeFlush(c, tableName, "d");

    Assert.assertEquals(3, FunctionalTestUtils.countRFiles(c, tableName));

    // drop files that start with A
    CompactionStrategyConfig csConfig = new CompactionStrategyConfig(TestCompactionStrategy.class.getName());
    csConfig.setOptions(ImmutableMap.of("inputPrefix", "F"));

    IteratorSetting iterConf = new IteratorSetting(21, "myregex", RegExFilter.class);
    RegExFilter.setRegexs(iterConf, "a|c", null, null, null, false);

    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig).setIterators(Arrays.asList(iterConf)));

    // compaction strategy should only be applied to one file. If its applied to both, then row 'b' would be dropped by filter.
    Assert.assertEquals(ImmutableSet.of("a", "b", "c"), getRows(c, tableName));

    Assert.assertEquals(2, FunctionalTestUtils.countRFiles(c, tableName));

    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    // ensure that iterator is not applied
    Assert.assertEquals(ImmutableSet.of("a", "b", "c"), getRows(c, tableName));

    Assert.assertEquals(1, FunctionalTestUtils.countRFiles(c, tableName));
  }

  @Test
  public void testFileSize() throws Exception {
    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    // write random data because its very unlikely it will compress
    writeRandomValue(c, tableName, 1 << 16);
    writeRandomValue(c, tableName, 1 << 16);

    writeRandomValue(c, tableName, 1 << 9);
    writeRandomValue(c, tableName, 1 << 7);
    writeRandomValue(c, tableName, 1 << 6);

    Assert.assertEquals(5, FunctionalTestUtils.countRFiles(c, tableName));

    CompactionStrategyConfig csConfig = new CompactionStrategyConfig(SizeCompactionStrategy.class.getName());
    csConfig.setOptions(ImmutableMap.of("size", "" + (1 << 15)));
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig));

    Assert.assertEquals(3, FunctionalTestUtils.countRFiles(c, tableName));

    csConfig = new CompactionStrategyConfig(SizeCompactionStrategy.class.getName());
    csConfig.setOptions(ImmutableMap.of("size", "" + (1 << 17)));
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setCompactionStrategy(csConfig));

    Assert.assertEquals(1, FunctionalTestUtils.countRFiles(c, tableName));

  }

  @Test
  public void testConcurrent() throws Exception {
    // two compactions without iterators or strategy should be able to run concurrently

    Connector c = getConnector();

    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    // write random data because its very unlikely it will compress
    writeRandomValue(c, tableName, 1 << 16);
    writeRandomValue(c, tableName, 1 << 16);

    c.tableOperations().compact(tableName, new CompactionConfig().setWait(false));
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    Assert.assertEquals(1, FunctionalTestUtils.countRFiles(c, tableName));

    writeRandomValue(c, tableName, 1 << 16);

    IteratorSetting iterConfig = new IteratorSetting(30, SlowIterator.class);
    SlowIterator.setSleepTime(iterConfig, 1000);

    long t1 = System.currentTimeMillis();
    c.tableOperations().compact(tableName, new CompactionConfig().setWait(false).setIterators(Arrays.asList(iterConfig)));
    try {
      // this compaction should fail because previous one set iterators
      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      if (System.currentTimeMillis() - t1 < 2000)
        Assert.fail("Expected compaction to fail because another concurrent compaction set iterators");
    } catch (AccumuloException e) {}
  }

  void writeRandomValue(Connector c, String tableName, int size) throws Exception {
    Random rand = new Random();

    byte data1[] = new byte[size];
    rand.nextBytes(data1);

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m1 = new Mutation("r" + rand.nextInt(909090));
    m1.put("data", "bl0b", new Value(data1));

    bw.addMutation(m1);
    bw.close();
    c.tableOperations().flush(tableName, null, null, true);
  }

  private Set<String> getRows(Connector c, String tableName) throws TableNotFoundException {
    Set<String> rows = new HashSet<>();
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      for (Entry<Key,Value> entry : scanner)
        rows.add(entry.getKey().getRowData().toString());
    }
    return rows;
  }

  private void writeFlush(Connector conn, String tablename, String row) throws Exception {
    BatchWriter bw = conn.createBatchWriter(tablename, new BatchWriterConfig());
    Mutation m = new Mutation(row);
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();
    conn.tableOperations().flush(tablename, null, null, true);
  }
}
