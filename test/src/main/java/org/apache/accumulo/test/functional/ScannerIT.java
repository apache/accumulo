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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Stopwatch;

/**
 *
 */
public class ScannerIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void testScannerReadaheadConfiguration() throws Exception {
    final String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(table);

    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());

    Mutation m = new Mutation("a");
    for (int i = 0; i < 10; i++) {
      m.put(Integer.toString(i), "", "");
    }

    bw.addMutation(m);
    bw.close();

    IteratorSetting cfg;
    Stopwatch sw;
    Iterator<Entry<Key,Value>> iterator;
    try (Scanner s = c.createScanner(table, new Authorizations())) {

      cfg = new IteratorSetting(100, SlowIterator.class);
      // A batch size of one will end up calling seek() for each element with no calls to next()
      SlowIterator.setSeekSleepTime(cfg, 100l);

      s.addScanIterator(cfg);
      // Never start readahead
      s.setReadaheadThreshold(Long.MAX_VALUE);
      s.setBatchSize(1);
      s.setRange(new Range());

      sw = new Stopwatch();
      iterator = s.iterator();

      sw.start();
      while (iterator.hasNext()) {
        sw.stop();

        // While we "do work" in the client, we should be fetching the next result
        UtilWaitThread.sleep(100l);
        iterator.next();
        sw.start();
      }
      sw.stop();
    }

    long millisWithWait = sw.elapsed(TimeUnit.MILLISECONDS);

    try (Scanner s = c.createScanner(table, new Authorizations())) {
      s.addScanIterator(cfg);
      s.setRange(new Range());
      s.setBatchSize(1);
      s.setReadaheadThreshold(0l);

      sw = new Stopwatch();
      iterator = s.iterator();

      sw.start();
      while (iterator.hasNext()) {
        sw.stop();

        // While we "do work" in the client, we should be fetching the next result
        UtilWaitThread.sleep(100l);
        iterator.next();
        sw.start();
      }
      sw.stop();

      long millisWithNoWait = sw.elapsed(TimeUnit.MILLISECONDS);

      // The "no-wait" time should be much less than the "wait-time"
      Assert.assertTrue("Expected less time to be taken with immediate readahead (" + millisWithNoWait + ") than without immediate readahead ("
          + millisWithWait + ")", millisWithNoWait < millisWithWait);
    }
  }

}
