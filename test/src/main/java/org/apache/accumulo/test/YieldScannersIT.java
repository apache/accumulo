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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.BatchScanner;
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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.YieldingIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ACCUMULO-4643
public class YieldScannersIT extends AccumuloClusterHarness {
  Logger log = LoggerFactory.getLogger(YieldScannersIT.class);
  private static final char START_ROW = 'a';

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void testScan() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    final BatchWriter writer = conn.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      byte[] row = new byte[] {(byte) (START_ROW + i)};
      Mutation m = new Mutation(new Text(row));
      m.put(new Text(), new Text(), new Value());
      writer.addMutation(m);
    }
    writer.flush();
    writer.close();

    log.info("Creating scanner");
    // make a scanner for a table with 10 keys
    final Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    final IteratorSetting cfg = new IteratorSetting(100, YieldingIterator.class);
    scanner.addScanIterator(cfg);

    log.info("iterating");
    Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
    int keyCount = 0;
    int yieldNextCount = 0;
    int yieldSeekCount = 0;
    while (it.hasNext()) {
      Map.Entry<Key,Value> next = it.next();
      log.info(Integer.toString(keyCount) + ": Got key " + next.getKey() + " with value " + next.getValue());

      // verify we got the expected key
      char expected = (char) (START_ROW + keyCount);
      Assert.assertEquals("Unexpected row", Character.toString(expected), next.getKey().getRow().toString());

      // determine whether we yielded on a next and seek
      if ((keyCount & 1) != 0) {
        yieldNextCount++;
        yieldSeekCount++;
      }
      String[] value = StringUtils.split(next.getValue().toString(), ',');
      Assert.assertEquals("Unexpected yield next count", Integer.toString(yieldNextCount), value[0]);
      Assert.assertEquals("Unexpected yield seek count", Integer.toString(yieldSeekCount), value[1]);
      Assert.assertEquals("Unexpected rebuild count", Integer.toString(yieldNextCount + yieldSeekCount), value[2]);

      keyCount++;
    }
    Assert.assertEquals("Did not get the expected number of results", 10, keyCount);
  }

  @Test
  public void testBatchScan() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    final BatchWriter writer = conn.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      byte[] row = new byte[] {(byte) (START_ROW + i)};
      Mutation m = new Mutation(new Text(row));
      m.put(new Text(), new Text(), new Value());
      writer.addMutation(m);
    }
    writer.flush();
    writer.close();

    log.info("Creating batch scanner");
    // make a scanner for a table with 10 keys
    final BatchScanner scanner = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1);
    final IteratorSetting cfg = new IteratorSetting(100, YieldingIterator.class);
    scanner.addScanIterator(cfg);
    scanner.setRanges(Collections.singleton(new Range()));

    log.info("iterating");
    Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
    int keyCount = 0;
    int yieldNextCount = 0;
    int yieldSeekCount = 0;
    while (it.hasNext()) {
      Map.Entry<Key,Value> next = it.next();
      log.info(Integer.toString(keyCount) + ": Got key " + next.getKey() + " with value " + next.getValue());

      // verify we got the expected key
      char expected = (char) (START_ROW + keyCount);
      Assert.assertEquals("Unexpected row", Character.toString(expected), next.getKey().getRow().toString());

      // determine whether we yielded on a next and seek
      if ((keyCount & 1) != 0) {
        yieldNextCount++;
        yieldSeekCount++;
      }
      String[] value = StringUtils.split(next.getValue().toString(), ',');
      Assert.assertEquals("Unexpected yield next count", Integer.toString(yieldNextCount), value[0]);
      Assert.assertEquals("Unexpected yield seek count", Integer.toString(yieldSeekCount), value[1]);
      Assert.assertEquals("Unexpected rebuild count", Integer.toString(yieldNextCount + yieldSeekCount), value[2]);

      keyCount++;
    }
    Assert.assertEquals("Did not get the expected number of results", 10, keyCount);
  }

}
