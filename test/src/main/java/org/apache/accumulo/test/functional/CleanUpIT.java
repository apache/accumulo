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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CleanUp;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that all threads spawned for ZooKeeper and Thrift connectivity are reaped after calling CleanUp.shutdown().
 *
 * Because this is destructive across the current context classloader, the normal teardown methods will fail (because they attempt to create a Connector). Until
 * the ZooKeeperInstance and Connector are self-contained WRT resource management, we can't leverage the AccumuloClusterBase.
 */
public class CleanUpIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(CleanUpIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void run() throws Exception {

    String tableName = getUniqueNames(1)[0];
    getConnector().tableOperations().create(tableName);

    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", 1, "5");

    bw.addMutation(m1);

    bw.flush();

    try (Scanner scanner = getConnector().createScanner(tableName, new Authorizations())) {

      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        count++;
        if (!entry.getValue().toString().equals("5")) {
          Assert.fail("Unexpected value " + entry.getValue());
        }
      }

      Assert.assertEquals("Unexpected count", 1, count);

      int threadCount = countThreads();
      if (threadCount < 2) {
        printThreadNames();
        Assert.fail("Not seeing expected threads. Saw " + threadCount);
      }

      CleanUp.shutdownNow();

      Mutation m2 = new Mutation("r2");
      m2.put("cf1", "cq1", 1, "6");

      try {
        bw.addMutation(m1);
        bw.flush();
        Assert.fail("batch writer did not fail");
      } catch (Exception e) {

      }

      try {
        // expect this to fail also, want to clean up batch writer threads
        bw.close();
        Assert.fail("batch writer close not fail");
      } catch (Exception e) {

      }

      try {
        count = 0;
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        while (iter.hasNext()) {
          iter.next();
          count++;
        }
        Assert.fail("scanner did not fail");
      } catch (Exception e) {

      }

      threadCount = countThreads();
      if (threadCount > 0) {
        printThreadNames();
        Assert.fail("Threads did not go away. Saw " + threadCount);
      }
    }
  }

  private void printThreadNames() {
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    Exception e = new Exception();
    for (Thread thread : threads) {
      e.setStackTrace(thread.getStackTrace());
      log.info("thread name: " + thread.getName(), e);
    }
  }

  /**
   * count threads that should be cleaned up
   *
   */
  private int countThreads() {
    int count = 0;
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    for (Thread thread : threads) {

      if (thread.getName().toLowerCase().contains("sendthread") || thread.getName().toLowerCase().contains("eventthread"))
        count++;

      if (thread.getName().toLowerCase().contains("thrift") && thread.getName().toLowerCase().contains("pool"))
        count++;
    }

    return count;
  }
}
