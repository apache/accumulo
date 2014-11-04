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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.client.Flush;
import org.apache.accumulo.examples.simple.client.RandomBatchScanner;
import org.apache.accumulo.examples.simple.client.RandomBatchWriter;
import org.apache.accumulo.examples.simple.client.ReadWriteExample;
import org.apache.accumulo.examples.simple.client.RowOperations;
import org.apache.accumulo.examples.simple.client.SequentialBatchWriter;
import org.apache.accumulo.examples.simple.client.TraceDumpExample;
import org.apache.accumulo.examples.simple.client.TracingExample;
import org.apache.accumulo.examples.simple.combiner.StatsCombiner;
import org.apache.accumulo.examples.simple.constraints.MaxMutationSize;
import org.apache.accumulo.examples.simple.dirlist.Ingest;
import org.apache.accumulo.examples.simple.dirlist.QueryUtil;
import org.apache.accumulo.examples.simple.helloworld.InsertWithBatchWriter;
import org.apache.accumulo.examples.simple.helloworld.ReadData;
import org.apache.accumulo.examples.simple.isolation.InterferenceTest;
import org.apache.accumulo.examples.simple.mapreduce.RegexExample;
import org.apache.accumulo.examples.simple.mapreduce.RowHash;
import org.apache.accumulo.examples.simple.mapreduce.TableToFile;
import org.apache.accumulo.examples.simple.mapreduce.TeraSortIngest;
import org.apache.accumulo.examples.simple.mapreduce.WordCount;
import org.apache.accumulo.examples.simple.mapreduce.bulk.BulkIngestExample;
import org.apache.accumulo.examples.simple.mapreduce.bulk.GenerateTestData;
import org.apache.accumulo.examples.simple.mapreduce.bulk.SetupTable;
import org.apache.accumulo.examples.simple.shard.ContinuousQuery;
import org.apache.accumulo.examples.simple.shard.Index;
import org.apache.accumulo.examples.simple.shard.Query;
import org.apache.accumulo.examples.simple.shard.Reverse;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl.LogWriter;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class ExamplesIT extends ConfigurableMacIT {
  private static final Logger log = Logger.getLogger(ExamplesIT.class);
  private static final BatchWriterOpts bwOpts = new BatchWriterOpts();
  private static final BatchWriterConfig bwc = new BatchWriterConfig();
  private static final String visibility = "A|B";
  private static final String auths = "A,B";

  Connector c;
  String instance;
  String keepers;
  String user = "root";
  String passwd;
  BatchWriter bw;
  IteratorSetting is;
  String dir;
  FileSystem fs;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopConf) {
    // 128MB * 3
    cfg.setDefaultMemory(cfg.getDefaultMemory() * 3, MemoryUnit.BYTE);
  }

  @Before
  public void getClusterInfo() throws Exception {
    c = getConnector();
    passwd = AbstractMacIT.ROOT_PASSWORD;
    fs = FileSystem.get(CachedConfiguration.getInstance());
    instance = c.getInstance().getInstanceName();
    keepers = c.getInstance().getZooKeepers();
    dir = cluster.getConfig().getDir().getAbsolutePath();

    c.securityOperations().changeUserAuthorizations(user, new Authorizations(auths.split(",")));
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Test
  public void testTrace() throws Exception {
    Process trace = cluster.exec(TraceServer.class);
    while (!c.tableOperations().exists("trace"))
      UtilWaitThread.sleep(500);
    Process p = goodExec(TracingExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-C", "-D", "-c");
    for (LogWriter writer : cluster.getLogWriters()) {
      writer.flush();
    }
    String result = FunctionalTestUtils.readAll(cluster, TracingExample.class, p);
    Pattern pattern = Pattern.compile("TraceID: ([0-9a-f]+)");
    Matcher matcher = pattern.matcher(result);
    int count = 0;
    while (matcher.find()) {
      p = goodExec(TraceDumpExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--traceid", matcher.group(1));
      count++;
    }
    assertTrue(count > 0);
    result = FunctionalTestUtils.readAll(cluster, TraceDumpExample.class, p);
    assertTrue(result.contains("myApp@myHost"));
    trace.destroy();
  }

  @Test
  public void testClasspath() throws Exception {
    Process p = cluster.exec(Main.class, Collections.singletonList(MapReduceIT.hadoopTmpDirArg), "classpath");
    assertEquals(0, p.waitFor());
    String result = FunctionalTestUtils.readAll(cluster, Main.class, p);
    int level1 = result.indexOf("Level 1");
    int level2 = result.indexOf("Level 2");
    int level3 = result.indexOf("Level 3");
    int level4 = result.indexOf("Level 4");
    assertTrue("Level 1 classloader not present.", level1 >= 0);
    assertTrue("Level 2 classloader not present.", level2 > 0);
    assertTrue("Level 3 classloader not present.", level3 > 0);
    assertTrue("Level 4 classloader not present.", level4 > 0);
    assertTrue(level1 < level2);
    assertTrue(level2 < level3);
    assertTrue(level3 < level4);
  }

  @Test
  public void testDirList() throws Exception {
    goodExec(Ingest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--dirTable", "dirTable", "--indexTable", "indexTable", "--dataTable",
        "dataTable", "--vis", visibility, "--chunkSize", 10000 + "", cluster.getConfig().getDir().getAbsolutePath());
    Process p = goodExec(QueryUtil.class, "-i", instance, "-z", keepers, "-p", passwd, "-u", user, "-t", "indexTable", "--auths", auths, "--search", "--path",
        "accumulo-site.xml");
    for (LogWriter writer : cluster.getLogWriters()) {
      writer.flush();
    }
    String result = FunctionalTestUtils.readAll(cluster, QueryUtil.class, p);
    log.info("result " + result);
    assertTrue(result.contains("accumulo-site.xml"));
  }

  @Test
  public void testAgeoffFilter() throws Exception {
    c.tableOperations().create("filtertest");
    is = new IteratorSetting(10, AgeOffFilter.class);
    AgeOffFilter.setTTL(is, 1000L);
    c.tableOperations().attachIterator("filtertest", is);
    UtilWaitThread.sleep(500); // let zookeeper updates propagate.
    bw = c.createBatchWriter("filtertest", bwc);
    Mutation m = new Mutation("foo");
    m.put("a", "b", "c");
    bw.addMutation(m);
    bw.close();
    UtilWaitThread.sleep(1000);
    assertEquals(0, FunctionalTestUtils.count(c.createScanner("filtertest", Authorizations.EMPTY)));
  }

  @Test
  public void testStatsCombiner() throws Exception {
    String table = "statscombinertest";
    c.tableOperations().create(table);
    is = new IteratorSetting(10, StatsCombiner.class);
    StatsCombiner.setCombineAllColumns(is, true);

    c.tableOperations().attachIterator(table, is);
    bw = c.createBatchWriter(table, bwc);
    Mutation m = new Mutation("foo");
    m.put("a", "b", "1");
    m.put("a", "b", "3");
    bw.addMutation(m);
    bw.flush();


    Iterator<Entry<Key, Value>> iter = c.createScanner(table, Authorizations.EMPTY).iterator();
    assertTrue("Iterator had no results", iter.hasNext());
    Entry<Key, Value> e = iter.next();
    assertEquals("Results ", "1,3,4,2", e.getValue().toString());
    assertFalse("Iterator had additional results", iter.hasNext());

    m = new Mutation("foo");
    m.put("a", "b", "0,20,20,2");
    bw.addMutation(m);
    bw.close();

    iter = c.createScanner(table, Authorizations.EMPTY).iterator();
    assertTrue("Iterator had no results", iter.hasNext());
    e = iter.next();
    assertEquals("Results ", "0,20,24,4", e.getValue().toString());
    assertFalse("Iterator had additional results", iter.hasNext());
  }

  @Test
  public void testBloomFilters() throws Exception {
    c.tableOperations().create("bloom_test");
    c.tableOperations().setProperty("bloom_test", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    goodExec(RandomBatchWriter.class, "--seed", "7", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "100000", "--min", "0", "--max",
        "1000000000", "--size", "50", "--batchMemory", "2M", "--batchLatency", "60s", "--batchThreads", "3", "-t", "bloom_test");
    c.tableOperations().flush("bloom_test", null, null, true);
    long diff = 0, diff2 = 0;
    // try the speed test a couple times in case the system is loaded with other tests
    for (int i = 0; i < 2; i++) {
      long now = System.currentTimeMillis();
      goodExec(RandomBatchScanner.class, "--seed", "7", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "10000", "--min", "0", "--max",
          "1000000000", "--size", "50", "--scanThreads", "4", "-t", "bloom_test");
      diff = System.currentTimeMillis() - now;
      now = System.currentTimeMillis();
      expectExec(1, RandomBatchScanner.class, "--seed", "8", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "10000", "--min", "0", "--max",
          "1000000000", "--size", "50", "--scanThreads", "4", "-t", "bloom_test");
      diff2 = System.currentTimeMillis() - now;
      if (diff2 < diff)
        break;
    }
    assertTrue(diff2 < diff);
  }

  @Test
  public void testShardedIndex() throws Exception {
    c.tableOperations().create("shard");
    c.tableOperations().create("doc2term");
    bw = c.createBatchWriter("shard", bwc);
    Index.index(30, new File(System.getProperty("user.dir") + "/src"), "\\W+", bw);
    bw.close();
    BatchScanner bs = c.createBatchScanner("shard", Authorizations.EMPTY, 4);
    List<String> found = Query.query(bs, Arrays.asList("foo", "bar"));
    bs.close();
    // should find ourselves
    boolean thisFile = false;
    for (String file : found) {
      if (file.endsWith("/ExamplesIT.java"))
        thisFile = true;
    }
    assertTrue(thisFile);
    // create a reverse index
    c.tableOperations().create("doc2Term");
    goodExec(Reverse.class, "-i", instance, "-z", keepers, "--shardTable", "shard", "--doc2Term", "doc2Term", "-u", "root", "-p", passwd);
    // run some queries
    goodExec(ContinuousQuery.class, "-i", instance, "-z", keepers, "--shardTable", "shard", "--doc2Term", "doc2Term", "-u", "root", "-p", passwd, "--terms",
        "5", "--count", "1000");
  }

  @Test
  public void testMaxMutationConstraint() throws Exception {
    c.tableOperations().create("test_ingest");
    c.tableOperations().addConstraint("test_ingest", MaxMutationSize.class.getName());
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 1;
    opts.cols = 1000;
    try {
      TestIngest.ingest(c, opts, bwOpts);
    } catch (MutationsRejectedException ex) {
      assertEquals(1, ex.getConstraintViolationSummaries().size());
    }
  }

  @Test
  public void testBulkIngest() throws Exception {
    goodExec(GenerateTestData.class, "--start-row", "0", "--count", "10000", "--output", dir + "/tmp/input/data");
    goodExec(SetupTable.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "bulkTable");
    goodExec(BulkIngestExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "bulkTable", "--inputDir", dir + "/tmp/input",
        "--workDir", dir + "/tmp");
  }

  @Test
  public void testTeraSortAndRead() throws Exception {
    String sorted = "sorted";
    goodExec(TeraSortIngest.class, "--count", (1000 * 1000) + "", "-nk", "10", "-xk", "10", "-nv", "10", "-xv", "10", "-t", sorted, "-i", instance, "-z",
        keepers, "-u", user, "-p", passwd, "--splits", "4");
    goodExec(RegexExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", sorted, "--rowRegex", ".*999.*", "--output", dir + "/tmp/nines");
    goodExec(RowHash.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", sorted, "--column", "c:");
    goodExec(TableToFile.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", sorted, "--output", dir + "/tmp/tableFile");
  }

  @Test
  public void testWordCount() throws Exception {
    c.tableOperations().create("wordCount");
    is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column(new Text("count"))));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator("wordCount", is);
    fs.copyFromLocalFile(new Path(new Path(System.getProperty("user.dir")).getParent(), "README"), new Path(dir + "/tmp/wc/README"));
    goodExec(WordCount.class, "-i", instance, "-u", user, "-p", passwd, "-z", keepers, "--input", dir + "/tmp/wc", "-t", "wordCount");
  }

  @Test
  public void testInsertWithBatchWriterAndReadData() throws Exception {
    String helloBatch = "helloBatch";
    goodExec(InsertWithBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", helloBatch);
    goodExec(ReadData.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", helloBatch);
  }

  @Test
  public void testIsolatedScansWithInterference() throws Exception {
    goodExec(InterferenceTest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "itest1", "--iterations", "100000", "--isolated");
  }

  @Test
  public void testScansWithInterference() throws Exception {
    goodExec(InterferenceTest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "itest2", "--iterations", "100000");
  }

  @Test
  public void testRowOperations() throws Exception {
    goodExec(RowOperations.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd);
  }

  @Test
  public void testBatchWriter() throws Exception {
    c.tableOperations().create("test");
    goodExec(SequentialBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "test", "--start", "0", "--num", "100000", "--size",
        "50", "--batchMemory", "10000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility);

  }

  @Test
  public void testReadWriteAndDelete() throws Exception {
    String test2 = "test2";
    goodExec(ReadWriteExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", test2, "--createtable", "-c",
        "--debug");
    goodExec(ReadWriteExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", test2, "-d", "--debug");

  }

  @Test
  public void testRandomBatchesAndFlush() throws Exception {
    String test3 = "test3";
    c.tableOperations().create(test3);
    goodExec(RandomBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", test3, "--num", "100000", "--min", "0", "--max",
        "100000", "--size", "100", "--batchMemory", "1000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility);
    goodExec(RandomBatchScanner.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", test3, "--num", "10000", "--min", "0", "--max",
        "100000", "--size", "100", "--scanThreads", "4", "--auths", auths);
    goodExec(Flush.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", test3);
  }

  private Process goodExec(Class<?> theClass, String... args) throws InterruptedException, IOException {
    return expectExec(0, theClass, args);
  }

  private Process expectExec(int exitCode, Class<?> theClass, String... args) throws InterruptedException, IOException {
    Process p = null;
    assertEquals(exitCode, (p = cluster.exec(theClass, Collections.singletonList(MapReduceIT.hadoopTmpDirArg), args)).waitFor());
    return p;
  }
}
