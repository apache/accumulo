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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchScanSplitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(BatchScanSplitIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "0");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    int numRows = 1 << 18;

    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < numRows; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value(String.format("%016x", numRows - i).getBytes(UTF_8)));
      bw.addMutation(m);
    }

    bw.close();

    getConnector().tableOperations().flush(tableName, null, null, true);

    getConnector().tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "4K");

    Collection<Text> splits = getConnector().tableOperations().listSplits(tableName);
    while (splits.size() < 2) {
      sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
      splits = getConnector().tableOperations().listSplits(tableName);
    }

    System.out.println("splits : " + splits);

    Random random = new Random(19011230);
    HashMap<Text,Value> expected = new HashMap<>();
    ArrayList<Range> ranges = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      int r = random.nextInt(numRows);
      Text row = new Text(String.format("%09x", r));
      expected.put(row, new Value(String.format("%016x", numRows - r).getBytes(UTF_8)));
      ranges.add(new Range(row));
    }

    // logger.setLevel(Level.TRACE);

    HashMap<Text,Value> found = new HashMap<>();

    for (int i = 0; i < 20; i++) {
      try (BatchScanner bs = getConnector().createBatchScanner(tableName, Authorizations.EMPTY, 4)) {

        found.clear();

        long t1 = System.currentTimeMillis();

        bs.setRanges(ranges);

        for (Entry<Key,Value> entry : bs) {
          found.put(entry.getKey().getRow(), entry.getValue());
        }

        long t2 = System.currentTimeMillis();

        log.info(String.format("rate : %06.2f%n", ranges.size() / ((t2 - t1) / 1000.0)));

        if (!found.equals(expected))
          throw new Exception("Found and expected differ " + found + " " + expected);
      }
    }

    splits = getConnector().tableOperations().listSplits(tableName);
    log.info("splits : {}", splits);
  }

}
