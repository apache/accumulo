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

import static com.google.common.base.Charsets.UTF_8;
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

import org.apache.accumulo.cluster.standalone.StandaloneClusterControl;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
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
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl.LogWriter;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class ExamplesIT extends AccumuloClusterIT {
  private static final Logger log = Logger.getLogger(ExamplesIT.class);
  private static final BatchWriterOpts bwOpts = new BatchWriterOpts();
  private static final BatchWriterConfig bwc = new BatchWriterConfig();
  // quoted to make sure the shell doesn't interpret the pipe
  private static final String visibility = "\"A|B\"";
  private static final String auths = "A,B";

  Connector c;
  String instance;
  String keepers;
  String user;
  String passwd;
  BatchWriter bw;
  IteratorSetting is;
  String dir;
  FileSystem fs;
  Authorizations origAuths;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopConf) {
    // 128MB * 3
    cfg.setDefaultMemory(cfg.getDefaultMemory() * 3, MemoryUnit.BYTE);
  }

  @Before
  public void getClusterInfo() throws Exception {
    c = getConnector();
    user = getPrincipal();
    Assume.assumeTrue(getToken() instanceof PasswordToken);
    passwd = new String(((PasswordToken) getToken()).getPassword(), UTF_8);
    fs = getCluster().getFileSystem();
    instance = c.getInstance().getInstanceName();
    keepers = c.getInstance().getZooKeepers();
    dir = getUsableDir();

    origAuths = c.securityOperations().getUserAuthorizations(user);
    c.securityOperations().changeUserAuthorizations(user, new Authorizations(auths.split(",")));
  }

  @After
  public void resetAuths() throws Exception {
    if (null != origAuths) {
      getConnector().securityOperations().changeUserAuthorizations(getPrincipal(), origAuths);
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Test
  public void testTrace() throws Exception {
    Process trace = null;
    if (ClusterType.MINI == getClusterType()) {
      MiniAccumuloClusterImpl impl = (MiniAccumuloClusterImpl) cluster;
      trace = impl.exec(TraceServer.class);
      while (!c.tableOperations().exists("trace"))
        UtilWaitThread.sleep(500);
    }
    Entry<Integer,String> pair = cluster.getClusterControl().execWithStdout(TracingExample.class,
        new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-C", "-D", "-c"});
    Assert.assertEquals("Expected return code of zero. STDOUT=" + pair.getValue(), 0, pair.getKey().intValue());
    String result = pair.getValue();
    Pattern pattern = Pattern.compile("TraceID: ([0-9a-f]+)");
    Matcher matcher = pattern.matcher(result);
    int count = 0;
    while (matcher.find()) {
      pair = cluster.getClusterControl().execWithStdout(TraceDumpExample.class,
          new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--traceid", matcher.group(1)});
      count++;
    }
    assertTrue(count > 0);
    assertTrue(pair.getValue().contains("myHost@myApp"));
    if (ClusterType.MINI == getClusterType() && null != trace) {
      trace.destroy();
    }
  }

  @Test
  public void testClasspath() throws Exception {
    // Process p = cluster.exec(Main.class, Collections.singletonList(MapReduceIT.hadoopTmpDirArg), "classpath");
    Entry<Integer,String> entry = getCluster().getClusterControl().execWithStdout(Main.class, new String[] {"classpath"});
    assertEquals(0, entry.getKey().intValue());
    String result = entry.getValue();
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
    String[] names = getUniqueNames(3);
    String dirTable = names[0], indexTable = names[1], dataTable = names[2];
    Entry<Integer,String> entry = getClusterControl().execWithStdout(
        Ingest.class,
        new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--dirTable", dirTable, "--indexTable", indexTable, "--dataTable", dataTable,
            "--vis", visibility, "--chunkSize", Integer.toString(10000), getUsableDir()});
    assertEquals("Got non-zero return code. Stdout=" + entry.getValue(), 0, entry.getKey().intValue());
    entry = getClusterControl().execWithStdout(QueryUtil.class,
        new String[] {"-i", instance, "-z", keepers, "-p", passwd, "-u", user, "-t", indexTable, "--auths", auths, "--search", "--path", "accumulo-site.xml"});
    if (ClusterType.MINI == getClusterType()) {
      MiniAccumuloClusterImpl impl = (MiniAccumuloClusterImpl) cluster;
      for (LogWriter writer : impl.getLogWriters()) {
        writer.flush();
      }
    }

    log.info("result " + entry.getValue());
    assertEquals(0, entry.getKey().intValue());
    assertTrue(entry.getValue().contains("accumulo-site.xml"));
  }

  @Test
  public void testAgeoffFilter() throws Exception {
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    is = new IteratorSetting(10, AgeOffFilter.class);
    AgeOffFilter.setTTL(is, 1000L);
    c.tableOperations().attachIterator(tableName, is);
    UtilWaitThread.sleep(500); // let zookeeper updates propagate.
    bw = c.createBatchWriter(tableName, bwc);
    Mutation m = new Mutation("foo");
    m.put("a", "b", "c");
    bw.addMutation(m);
    bw.close();
    UtilWaitThread.sleep(1000);
    assertEquals(0, FunctionalTestUtils.count(c.createScanner(tableName, Authorizations.EMPTY)));
  }

  @Test
  public void testStatsCombiner() throws Exception {
    String table = getUniqueNames(1)[0];
    c.tableOperations().create(table);
    is = new IteratorSetting(10, StatsCombiner.class);
    StatsCombiner.setCombineAllColumns(is, true);

    c.tableOperations().attachIterator(table, is);
    bw = c.createBatchWriter(table, bwc);
    // Write two mutations otherwise the NativeMap would dedupe them into a single update
    Mutation m = new Mutation("foo");
    m.put("a", "b", "1");
    bw.addMutation(m);
    m = new Mutation("foo");
    m.put("a", "b", "3");
    bw.addMutation(m);
    bw.flush();

    Iterator<Entry<Key,Value>> iter = c.createScanner(table, Authorizations.EMPTY).iterator();
    assertTrue("Iterator had no results", iter.hasNext());
    Entry<Key,Value> e = iter.next();
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
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    goodExec(RandomBatchWriter.class, "--seed", "7", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "100000", "--min", "0", "--max",
        "1000000000", "--size", "50", "--batchMemory", "2M", "--batchLatency", "60s", "--batchThreads", "3", "-t", tableName);
    c.tableOperations().flush(tableName, null, null, true);
    long diff = 0, diff2 = 0;
    // try the speed test a couple times in case the system is loaded with other tests
    for (int i = 0; i < 2; i++) {
      long now = System.currentTimeMillis();
      goodExec(RandomBatchScanner.class, "--seed", "7", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "10000", "--min", "0", "--max",
          "1000000000", "--size", "50", "--scanThreads", "4", "-t", tableName);
      diff = System.currentTimeMillis() - now;
      now = System.currentTimeMillis();
      int retCode = getClusterControl().exec(
          RandomBatchScanner.class,
          new String[] {"--seed", "8", "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--num", "10000", "--min", "0", "--max", "1000000000",
              "--size", "50", "--scanThreads", "4", "-t", tableName});
      assertEquals(1, retCode);
      diff2 = System.currentTimeMillis() - now;
      if (diff2 < diff)
        break;
    }
    assertTrue(diff2 < diff);
  }

  @Test
  public void testShardedIndex() throws Exception {
    String[] names = getUniqueNames(3);
    final String shard = names[0], index = names[1];
    c.tableOperations().create(shard);
    c.tableOperations().create(index);
    bw = c.createBatchWriter(shard, bwc);
    Index.index(30, new File(System.getProperty("user.dir") + "/src"), "\\W+", bw);
    bw.close();
    BatchScanner bs = c.createBatchScanner(shard, Authorizations.EMPTY, 4);
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
    goodExec(Reverse.class, "-i", instance, "-z", keepers, "--shardTable", shard, "--doc2Term", index, "-u", getPrincipal(), "-p", passwd);
    // run some queries
    goodExec(ContinuousQuery.class, "-i", instance, "-z", keepers, "--shardTable", shard, "--doc2Term", index, "-u", "root", "-p", passwd, "--terms", "5",
        "--count", "1000");
  }

  @Test
  public void testMaxMutationConstraint() throws Exception {
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().addConstraint(tableName, MaxMutationSize.class.getName());
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 1;
    opts.cols = 1000;
    opts.tableName = tableName;
    try {
      TestIngest.ingest(c, opts, bwOpts);
    } catch (MutationsRejectedException ex) {
      assertEquals(1, ex.getConstraintViolationSummaries().size());
    }
  }

  @Test
  public void testBulkIngest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    FileSystem fs = getFileSystem();
    Path p = new Path(dir, "tmp");
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    goodExec(GenerateTestData.class, "--start-row", "0", "--count", "10000", "--output", dir + "/tmp/input/data");
    goodExec(SetupTable.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", tableName);
    goodExec(BulkIngestExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", tableName, "--inputDir", dir + "/tmp/input",
        "--workDir", dir + "/tmp");
  }

  @Test
  public void testTeraSortAndRead() throws Exception {
    String tableName = getUniqueNames(1)[0];
    goodExec(TeraSortIngest.class, "--count", (1000 * 1000) + "", "-nk", "10", "-xk", "10", "-nv", "10", "-xv", "10", "-t", tableName, "-i", instance, "-z",
        keepers, "-u", user, "-p", passwd, "--splits", "4");
    Path output = new Path(dir, "tmp/nines");
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    goodExec(RegexExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName, "--rowRegex", ".*999.*", "--output",
        output.toString());
    goodExec(RowHash.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName, "--column", "c:");
    output = new Path(dir, "tmp/tableFile");
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    goodExec(TableToFile.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName, "--output", output.toString());
  }

  @Test
  public void testWordCount() throws Exception {
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column(new Text("count"))));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator(tableName, is);
    fs.copyFromLocalFile(new Path(new Path(System.getProperty("user.dir")).getParent(), "README"), new Path(dir + "/tmp/wc/README"));
    goodExec(WordCount.class, "-i", instance, "-u", user, "-p", passwd, "-z", keepers, "--input", dir + "/tmp/wc", "-t", tableName);
  }

  @Test
  public void testInsertWithBatchWriterAndReadData() throws Exception {
    String tableName = getUniqueNames(1)[0];
    goodExec(InsertWithBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName);
    goodExec(ReadData.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName);
  }

  @Test
  public void testIsolatedScansWithInterference() throws Exception {
    goodExec(InterferenceTest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", getUniqueNames(1)[0], "--iterations", "100000",
        "--isolated");
  }

  @Test
  public void testScansWithInterference() throws Exception {
    goodExec(InterferenceTest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", getUniqueNames(1)[0], "--iterations", "100000");
  }

  @Test
  public void testRowOperations() throws Exception {
    goodExec(RowOperations.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd);
  }

  @Test
  public void testBatchWriter() throws Exception {
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    goodExec(SequentialBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", tableName, "--start", "0", "--num", "100000",
        "--size", "50", "--batchMemory", "10000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility);

  }

  @Test
  public void testReadWriteAndDelete() throws Exception {
    String tableName = getUniqueNames(1)[0];
    goodExec(ReadWriteExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", tableName, "--createtable", "-c",
        "--debug");
    goodExec(ReadWriteExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", tableName, "-d", "--debug");

  }

  @Test
  public void testRandomBatchesAndFlush() throws Exception {
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    goodExec(RandomBatchWriter.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", tableName, "--num", "100000", "--min", "0", "--max",
        "100000", "--size", "100", "--batchMemory", "1000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility);
    goodExec(RandomBatchScanner.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", tableName, "--num", "10000", "--min", "0", "--max",
        "100000", "--size", "100", "--scanThreads", "4", "--auths", auths);
    goodExec(Flush.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", tableName);
  }

  private void goodExec(Class<?> theClass, String... args) throws InterruptedException, IOException {
    Entry<Integer,String> pair;
    if (Tool.class.isAssignableFrom(theClass) && ClusterType.STANDALONE == getClusterType()) {
      StandaloneClusterControl control = (StandaloneClusterControl) getClusterControl();
      pair = control.execMapreduceWithStdout(theClass, args);
    } else {
      // We're already slurping stdout into memory (not redirecting to file). Might as well add it to error message.
      pair = getClusterControl().execWithStdout(theClass, args);
    }
    Assert.assertEquals("stdout=" + pair.getValue(), 0, pair.getKey().intValue());
  }
}
