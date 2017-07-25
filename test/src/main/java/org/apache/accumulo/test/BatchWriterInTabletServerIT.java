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

import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

/**
 * Test writing to another table from inside an iterator.
 *
 * @see BatchWriterIterator
 */
public class BatchWriterInTabletServerIT extends AccumuloClusterHarness {
  private static final Logger log = Logger.getLogger(BatchWriterInTabletServerIT.class);

  @Override
  public boolean canRunTest(ClusterType type) {
    return ClusterType.MINI == type;
  }

  /**
   * This test should succeed.
   */
  @Test
  public void testNormalWrite() throws Exception {
    String[] uniqueNames = getUniqueNames(2);
    String t1 = uniqueNames[0], t2 = uniqueNames[1];
    Connector c = getConnector();
    int numEntriesToWritePerEntry = 50;
    IteratorSetting itset = BatchWriterIterator.iteratorSetting(6, 0, 15, 1000, numEntriesToWritePerEntry, t2, c, getAdminToken(), false, false);
    test(t1, t2, c, itset, numEntriesToWritePerEntry);
  }

  /**
   * Fixed by ACCUMULO-4229.
   * <p>
   * This tests a situation that a client which shares a LocatorCache with the tablet server may fall into. Before the problem was fixed, adding a split after
   * the Locator cache falls out of sync caused the BatchWriter to continuously attempt to write to an old, closed tablet. It would do so for 15 seconds until a
   * timeout on the BatchWriter.
   */
  @Test
  public void testClearLocatorAndSplitWrite() throws Exception {
    String[] uniqueNames = getUniqueNames(2);
    String t1 = uniqueNames[0], t2 = uniqueNames[1];
    Connector c = getConnector();
    int numEntriesToWritePerEntry = 50;
    IteratorSetting itset = BatchWriterIterator.iteratorSetting(6, 0, 15, 1000, numEntriesToWritePerEntry, t2, c, getAdminToken(), true, true);
    test(t1, t2, c, itset, numEntriesToWritePerEntry);
  }

  private void test(String t1, String t2, Connector c, IteratorSetting itset, int numEntriesToWritePerEntry) throws Exception {
    // Write an entry to t1
    c.tableOperations().create(t1);
    Key k = new Key(new Text("row"), new Text("cf"), new Text("cq"));
    Value v = new Value("1".getBytes());
    {
      BatchWriterConfig config = new BatchWriterConfig();
      config.setMaxMemory(0);
      BatchWriter writer = c.createBatchWriter(t1, config);
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), v);
      writer.addMutation(m);
      writer.close();
    }

    // Create t2 with a combiner to count entries written to it
    c.tableOperations().create(t2);
    IteratorSetting summer = new IteratorSetting(2, "summer", SummingCombiner.class);
    LongCombiner.setEncodingType(summer, LongCombiner.Type.STRING);
    LongCombiner.setCombineAllColumns(summer, true);
    c.tableOperations().attachIterator(t2, summer);

    Map.Entry<Key,Value> actual;
    // Scan t1 with an iterator that writes to table t2
    Scanner scanner = c.createScanner(t1, Authorizations.EMPTY);
    scanner.addScanIterator(itset);
    actual = Iterators.getOnlyElement(scanner.iterator());
    Assert.assertTrue(actual.getKey().equals(k, PartialKey.ROW_COLFAM_COLQUAL));
    Assert.assertEquals(BatchWriterIterator.SUCCESS_VALUE, actual.getValue());
    scanner.close();

    // ensure entries correctly wrote to table t2
    scanner = c.createScanner(t2, Authorizations.EMPTY);
    actual = Iterators.getOnlyElement(scanner.iterator());
    log.debug("t2 entry is " + actual.getKey().toStringNoTime() + " -> " + actual.getValue());
    Assert.assertTrue(actual.getKey().equals(k, PartialKey.ROW_COLFAM_COLQUAL));
    Assert.assertEquals(numEntriesToWritePerEntry, Integer.parseInt(actual.getValue().toString()));
    scanner.close();

    c.tableOperations().delete(t1);
    c.tableOperations().delete(t2);
  }

}
