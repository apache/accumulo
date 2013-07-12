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
import org.apache.accumulo.minicluster.MiniAccumuloCluster.LogWriter;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ExamplesIT extends MacTest {
  
  BatchWriterOpts bwOpts = new BatchWriterOpts();
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setDefaultMemory(cfg.getDefaultMemory() * 2, MemoryUnit.BYTE);
  }
  
  @Test(timeout=5*60*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    String instance = c.getInstance().getInstanceName();
    String keepers = c.getInstance().getZooKeepers();
    String user = "root";
    String passwd = MacTest.PASSWORD;
    String visibility = "A|B";
    String auths = "A,B";
    BatchWriterConfig bwc = new BatchWriterConfig();
    BatchWriter bw;
    IteratorSetting is;
    String dir = cluster.getConfig().getDir().getAbsolutePath();
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    
    
    log.info("testing dirlist example (a little)");
    c.securityOperations().changeUserAuthorizations(user, new Authorizations(auths.split(",")));
    assertEquals(0, cluster.exec(Ingest.class, "-i", instance, "-z", keepers, "-u", user, "-p", passwd, 
        "--dirTable", "dirTable", "--indexTable", "indexTable", "--dataTable", "dataTable",
        "--vis", visibility, "--chunkSize", 10000 + "", cluster.getConfig().getDir().getAbsolutePath()).waitFor());
    Process p = cluster.exec(QueryUtil.class, "-i", instance, "-z", keepers, "-p", passwd, "-u", user,
        "-t", "indexTable", "--auths", auths, "--search", "--path", "accumulo-site.xml");
    assertEquals(0, p.waitFor());
    for(LogWriter writer : cluster.getLogWriters()) {
      writer.flush();
    }
    String result = FunctionalTestUtils.readAll(cluster, QueryUtil.class, p);
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
    int count = 0;
    for (@SuppressWarnings("unused") Entry<Key,Value> line : c.createScanner("filtertest", Authorizations.EMPTY))
      count++;
    assertEquals(0, count);
    
    
    log.info("Testing bloom filters are fast for missing data");
    c.tableOperations().create("bloom_test");
    c.tableOperations().setProperty("bloom_test", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    assertEquals(0, cluster.exec(RandomBatchWriter.class, "--seed", "7", "-i", instance, "-z",
        keepers, "-u", user, "-p", MacTest.PASSWORD, "--num", "100000", "--min", "0", "--max", "1000000000", "--size", "50",
        "--batchMemmory", "2M", "--batchLatency", "60s", "--batchThreads", "3", "-t", "bloom_test").waitFor());
    c.tableOperations().flush("bloom_test", null, null, true);
    long now = System.currentTimeMillis();
    assertEquals(0,  cluster.exec(RandomBatchScanner.class,"--seed", "7", "-i", instance, "-z",
        keepers, "-u", user, "-p", MacTest.PASSWORD, "--num", "10000", "--min", "0", "--max", "1000000000", "--size", "50",
        "--scanThreads", "4","-t", "bloom_test").waitFor());
    long diff = System.currentTimeMillis() - now;
    now = System.currentTimeMillis();
    assertEquals(0,  cluster.exec(RandomBatchScanner.class,"--seed", "8", "-i", instance, "-z",
        keepers, "-u", user, "-p", MacTest.PASSWORD, "--num", "10000", "--min", "0", "--max", "1000000000", "--size", "50",
        "--scanThreads", "4","-t", "bloom_test").waitFor());
    long diff2 = System.currentTimeMillis() - now;
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
    assertEquals(0, cluster.exec(Reverse.class, "-i", instance, "-z", keepers, "-t", "shard", "--doc2Term",
        "-u", "root", "-p", passwd).waitFor());
    // run some queries
    assertEquals(0, cluster.exec(ContinuousQuery.class, "-i", instance, "-z", keepers, "-t", "shard", "--doc2Term",
        "-u", "root", "-p", passwd, "--term", "5", "--count", "1000").waitFor());
    
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

    log.info("Starting build ingest example");
    assertEquals(0, cluster.exec(GenerateTestData.class, "0", "10000", dir + "/tmp/input/data").waitFor());
    assertEquals(0, cluster.exec(SetupTable.class, instance, keepers, user, passwd, "bulkTable").waitFor());
    assertEquals(0, cluster.exec(BulkIngestExample.class, instance, keepers, user, passwd, "bulkTable",
        dir + "/tmp/input", dir + "/tmp").waitFor());
    assertEquals(0, cluster.exec(VerifyIngest.class, instance, keepers, user, passwd, "bulkTable", "0", "1000000").waitFor());
    
    log.info("Starting bulk ingest example");
    assertEquals(0, cluster.exec(GenerateTestData.class, "0", "1000000", dir + "/tmp/input/data").waitFor());
    assertEquals(0, cluster.exec(SetupTable.class, instance, keepers, user, passwd, "bulkTable").waitFor());
    assertEquals(0, cluster.exec(BulkIngestExample.class, instance, keepers, user, passwd, "bulkTable", dir + "/tmp/input", dir + "/tmp").waitFor());
    assertEquals(0, cluster.exec(VerifyIngest.class, instance, keepers, user, passwd, "bulkTable", "0", "1000000").waitFor());

    log.info("Running TeraSortIngest example");
    TeraSortIngest.main(new String[]{
        "--count", (1000*1000) + "",
        "-nk", "10", "-xk", "10",
        "-nv", "10", "-xv", "10",
        "-t", "sorted",
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--splits", "4"});
    log.info("Running Regex example");
    RegexExample.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "sorted",
        "--rowRegex", ".*999.*",
        "--output", dir + "/tmp/nines"
    });
    log.info("Running RowHash example");
    RowHash.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "sorted",
        "--column", "c:"
    });
    log.info("Running TableToFile example");
    TableToFile.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "sorted",
        "--output", dir + "/tmp/tableFile"
    });

    log.info("Running word count example");
    c.tableOperations().create("wordCount");
    is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column(new Text("count"))));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator("wordCount", is);
    fs.copyFromLocalFile(new Path(new Path(System.getProperty("user.dir")).getParent(), "README"), new Path(dir + "/tmp/wc/README"));
    WordCount.main(new String[] {
       "-i", instance,
       "-u", user,
       "-p", passwd,
       "-z", keepers,
       "--input", dir + "/tmp/wc",
       "-t", "wordCount"
    });

    log.info("Inserting data with a batch writer");
    InsertWithBatchWriter.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "helloBatch"
    });
    log.info("Reading data");
    ReadData.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "helloBatch"
    });
    log.info("Running isolated scans");
    InterferenceTest.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "itest1",
        "--iterations", "100000",
        "--isolated"
    });
    log.info("Running scans without isolation");
    InterferenceTest.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "itest2",
        "--iterations", "100000",
    });
    log.info("Performing some row operations");
    RowOperations.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
    });
    log.info("Using the batch writer");
    c.tableOperations().create("test");
    SequentialBatchWriter.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "-t", "test",
        "--start", "0",
        "--num", "100000",
        "--size", "50",
        "--batchMemory", "10000000",
        "--batchLatency", "1000",
        "--batchThreads", "4",
        "--vis", visibility
    });

    log.info("Reading and writing some data");
    ReadWriteExample.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--auths", auths,
        "--table", "test2",
        "--createtable",
        "-c",
        "--debug"});
    log.info("Deleting some data");
    ReadWriteExample.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--auths", auths,
        "--table", "test2",
        "-d",
        "--debug"});
    log.info("Writing some data with the batch writer");
    c.tableOperations().create("test3");
    RandomBatchWriter.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--table", "test3",
        "--num", "100000",
        "--min", "0",
        "--max", "99999",
        "--size", "100",
        "--batchMemory", "1000000",
        "--batchLatency", "1000",
        "--batchThreads", "4",
        "--vis", visibility});
    log.info("Reading some data with the batch scanner");
    RandomBatchScanner.main(new String[] {
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--table", "test3",
        "--num", "10000",
        "--min", "0",
        "--max", "99999",
        "--size", "100",
        "--scanThreads", "4",
        "--auths", auths});
    log.info("Running an example table operation (Flush)");
    Flush.main(new String[]{
        "-i", instance,
        "-z", keepers,
        "-u", user,
        "-p", passwd,
        "--table", "test3",
    });
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());

  }

}
