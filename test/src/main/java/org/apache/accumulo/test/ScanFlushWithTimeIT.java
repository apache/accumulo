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

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanFlushWithTimeIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ScanFlushWithTimeIT.class);

  @Test(timeout = 30 * 1000)
  public void test() throws Exception {
    log.info("Creating table");
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    log.info("Adding slow iterator");
    IteratorSetting setting = new IteratorSetting(50, SlowIterator.class);
    SlowIterator.setSleepTime(setting, 1000);
    c.tableOperations().attachIterator(tableName, setting);
    log.info("Splitting the table");
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("5"));
    c.tableOperations().addSplits(tableName, partitionKeys);
    log.info("waiting for zookeeper propagation");
    UtilWaitThread.sleep(5 * 1000);
    log.info("Adding a few entries");
    BatchWriter bw = c.createBatchWriter(tableName, null);
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("" + i);
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
    log.info("Fetching some entries: should timeout and return something");

    log.info("Scanner");
    try (Scanner s = c.createScanner(tableName, Authorizations.EMPTY)) {
      s.setBatchTimeout(500, TimeUnit.MILLISECONDS);
      testScanner(s, 1200);

      log.info("IsolatedScanner");
      IsolatedScanner is = new IsolatedScanner(s);
      is.setReadaheadThreshold(1);
      // buffers an entire row
      testScanner(is, 2200);
    }

    log.info("BatchScanner");
    try (BatchScanner bs = c.createBatchScanner(tableName, Authorizations.EMPTY, 5)) {
      bs.setBatchTimeout(500, TimeUnit.MILLISECONDS);
      bs.setRanges(Collections.singletonList(new Range()));
      testScanner(bs, 1200);
    }
  }

  private void testScanner(ScannerBase s, long expected) {
    long now = System.currentTimeMillis();
    try {
      s.iterator().next();
    } finally {
      s.close();
    }
    long diff = System.currentTimeMillis() - now;
    log.info("Diff = {}", diff);
    assertTrue("Scanner taking too long to return intermediate results: " + diff, diff < expected);
  }
}
