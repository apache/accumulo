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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.accumulo.minicluster.MiniAccumuloCluster.LogWriter;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ExamplesIT extends ConfigurableMacIT {

  BatchWriterOpts bwOpts = new BatchWriterOpts();

  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setDefaultMemory(cfg.getDefaultMemory() * 2, MemoryUnit.BYTE);
  }

  @Test(timeout = 10 * 60 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    String instance = c.getInstance().getInstanceName();
    String keepers = c.getInstance().getZooKeepers();
    String user = "root";
    String passwd = ROOT_PASSWORD;
    String visibility = "A|B";
    String auths = "A,B";
    BatchWriterConfig bwc = new BatchWriterConfig();
    BatchWriter bw;
    IteratorSetting is;
    String dir = cluster.getConfig().getDir().getAbsolutePath();
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());

    Process trace = cluster.exec(TraceServer.class);
    while (!c.tableOperations().exists("trace"))
      UtilWaitThread.sleep(500);

    log.info("trace example");
    Process p = cluster.exec(TracingExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-C", "-D", "-c");
    assertEquals(0, p.waitFor());
    for (LogWriter writer : cluster.getLogWriters()) {
      writer.flush();
    }
    String result = FunctionalTestUtils.readAll(cluster, TracingExample.class, p);
    Pattern pattern = Pattern.compile("TraceID: ([0-9a-f]+)");
    Matcher matcher = pattern.matcher(result);
    int count = 0;
    while (matcher.find()) {
      p = cluster.exec(TraceDumpExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--traceid", matcher.group(1));
      assertEquals(0, p.waitFor());
      count++;
    }
    assertTrue(count > 0);
    result = FunctionalTestUtils.readAll(cluster, TraceDumpExample.class, p);
    assertTrue(result.contains("myHost@myApp"));
    trace.destroy();

    log.info("testing dirlist example (a little)");
    c.securityOperations().changeUserAuthorizations(user, new Authorizations(auths.split(",")));
    assertEquals(
        0,
        cluster.exec(Ingest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--dirTable", "dirTable", "--indexTable", "indexTable",
            "--dataTable", "dataTable", "--vis", visibility, "--chunkSize", 10000 + "", cluster.getConfig().getDir().getAbsolutePath()).waitFor());
    p = cluster.exec(QueryUtil.class, "-i", instance, "-z", keepers, "-p", passwd, "-u", user, "-t", "indexTable", "--auths", auths, "--search", "--path",
        "accumulo-site.xml");
    assertEquals(0, p.waitFor());
    for (LogWriter writer : cluster.getLogWriters()) {
      writer.flush();
    }
    result = FunctionalTestUtils.readAll(cluster, QueryUtil.class, p);
    System.out.println("result " + result);
    assertTrue(result.contains("accumulo-site.xml"));

    log.info("Testing ageoff filtering");
    c.tableOperations().create("filtertest");
    is = new IteratorSetting(10, AgeOffFilter.class);
    AgeOffFilter.setTTL(is, 1000L);
    c.tableOperations().attachIterator("filtertest", is);
    bw = c.createBatchWriter("filtertest", bwc);
    Mutation m = new Mutation("foo");
    m.put("a", "b", "c");
    bw.addMutation(m);
    UtilWaitThread.sleep(1000);
    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> line : c.createScanner("filtertest", Authorizations.EMPTY))
      count++;
    assertEquals(0, count);

    log.info("Testing bloom filters are fast for missing data");
    c.tableOperations().create("bloom_test");
    c.tableOperations().setProperty("bloom_test", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    assertEquals(
        0,
        cluster.exec(RandomBatchWriter.class, "--seed", "7", "-i", instance, "-z", keepers, "-u", user, "-p", ROOT_PASSWORD, "--num", "100000", "--min", "0",
            "--max", "1000000000", "--size", "50", "--batchMemory", "2M", "--batchLatency", "60s", "--batchThreads", "3", "-t", "bloom_test").waitFor());
    c.tableOperations().flush("bloom_test", null, null, true);
    long diff = 0, diff2 = 0;
    // try the speed test a couple times in case the system is loaded with other tests
    for (int i = 0; i < 2; i++) {
      long now = System.currentTimeMillis();
      assertEquals(0,  cluster.exec(RandomBatchScanner.class,"--seed", "7", "-i", instance, "-z",
          keepers, "-u", user, "-p", ROOT_PASSWORD, "--num", "10000", "--min", "0", "--max", "1000000000", "--size", "50",
          "--scanThreads", "4","-t", "bloom_test").waitFor());
      diff = System.currentTimeMillis() - now;
      now = System.currentTimeMillis();
      assertEquals(1,  cluster.exec(RandomBatchScanner.class,"--seed", "8", "-i", instance, "-z",
          keepers, "-u", user, "-p", ROOT_PASSWORD, "--num", "10000", "--min", "0", "--max", "1000000000", "--size", "50",
          "--scanThreads", "4","-t", "bloom_test").waitFor());
      diff2 = System.currentTimeMillis() - now;
      if (diff2 < diff)
        break;
    }
    assertTrue(diff2 < diff);

    log.info("Creating a sharded index of the accumulo java files");
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
    assertEquals(0, cluster.exec(Reverse.class, "-i", instance, "-z", keepers, "--shardTable", "shard", "--doc2Term", "doc2Term", "-u", "root", "-p", passwd).waitFor());
    // run some queries
    assertEquals(
        0,
        cluster.exec(ContinuousQuery.class, "-i", instance, "-z", keepers, "--shardTable", "shard", "--doc2Term", "doc2Term", "-u", "root", "-p", passwd, "--terms", "5", "--count",
            "1000").waitFor());

    log.info("Testing MaxMutation constraint");
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

    log.info("Starting bulk ingest example");
    assertEquals(0, cluster.exec(GenerateTestData.class, "--start-row", "0", "--count", "10000", "--output", dir + "/tmp/input/data").waitFor());
    assertEquals(0, cluster.exec(SetupTable.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "bulkTable").waitFor());
    assertEquals(0, cluster.exec(BulkIngestExample.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "bulkTable", "--inputDir", dir + "/tmp/input", "--workDir", dir + "/tmp").waitFor());

    log.info("Running TeraSortIngest example");
    exec(TeraSortIngest.class, new String[] {"--count", (1000 * 1000) + "", "-nk", "10", "-xk", "10", "-nv", "10", "-xv", "10", "-t", "sorted", "-i", instance,
        "-z", keepers, "-u", user, "-p", passwd, "--splits", "4"});
    log.info("Running Regex example");
    exec(RegexExample.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "sorted", "--rowRegex", ".*999.*", "--output",
        dir + "/tmp/nines"});
    log.info("Running RowHash example");
    exec(RowHash.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "sorted", "--column", "c:"});
    log.info("Running TableToFile example");
    exec(TableToFile.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "sorted", "--output", dir + "/tmp/tableFile"});

    log.info("Running word count example");
    c.tableOperations().create("wordCount");
    is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column(new Text("count"))));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator("wordCount", is);
    fs.copyFromLocalFile(new Path(new Path(System.getProperty("user.dir")).getParent(), "README"), new Path(dir + "/tmp/wc/README"));
    exec(WordCount.class, new String[] {"-i", instance, "-u", user, "-p", passwd, "-z", keepers, "--input", dir + "/tmp/wc", "-t", "wordCount"});

    log.info("Inserting data with a batch writer");
    exec(InsertWithBatchWriter.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "helloBatch"});
    log.info("Reading data");
    exec(ReadData.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "helloBatch"});
    log.info("Running isolated scans");
    exec(InterferenceTest.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "itest1", "--iterations", "100000", "--isolated"});
    log.info("Running scans without isolation");
    exec(InterferenceTest.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "itest2", "--iterations", "100000",});
    log.info("Performing some row operations");
    exec(RowOperations.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd,});
    log.info("Using the batch writer");
    c.tableOperations().create("test");
    exec(SequentialBatchWriter.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "-t", "test", "--start", "0", "--num", "100000",
        "--size", "50", "--batchMemory", "10000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility});

    log.info("Reading and writing some data");
    exec(ReadWriteExample.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", "test2", "--createtable",
        "-c", "--debug"});
    log.info("Deleting some data");
    exec(ReadWriteExample.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--auths", auths, "--table", "test2", "-d", "--debug"});
    log.info("Writing some data with the batch writer");
    c.tableOperations().create("test3");
    exec(RandomBatchWriter.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "test3", "--num", "100000", "--min", "0",
        "--max", "99999", "--size", "100", "--batchMemory", "1000000", "--batchLatency", "1000", "--batchThreads", "4", "--vis", visibility});
    log.info("Reading some data with the batch scanner");
    exec(RandomBatchScanner.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "test3", "--num", "10000", "--min", "0",
        "--max", "99999", "--size", "100", "--scanThreads", "4", "--auths", auths});
    log.info("Running an example table operation (Flush)");
    exec(Flush.class, new String[] {"-i", instance, "-z", keepers, "-u", user, "-p", passwd, "--table", "test3",});
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());

  }

}
