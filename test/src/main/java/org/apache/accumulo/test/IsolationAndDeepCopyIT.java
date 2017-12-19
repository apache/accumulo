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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class IsolationAndDeepCopyIT extends AccumuloClusterHarness {

  @Test
  public void testBugFix() throws Exception {
    // test bug fox for ACCUMULO-3977

    String table = super.getUniqueNames(1)[0];
    Connector conn = getConnector();

    conn.tableOperations().create(table);

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

    addDocument(bw, "000A", "dog", "cat", "hamster", "iguana", "the");
    addDocument(bw, "000B", "java", "perl", "C++", "pascal", "the");
    addDocument(bw, "000C", "chrome", "firefox", "safari", "opera", "the");
    addDocument(bw, "000D", "logarithmic", "quadratic", "linear", "exponential", "the");

    bw.close();

    // its a bug when using rfiles, so flush
    conn.tableOperations().flush(table, null, null, true);

    IteratorSetting iterCfg = new IteratorSetting(30, "ayeaye", IntersectingIterator.class.getName());
    IntersectingIterator.setColumnFamilies(iterCfg, new Text[] {new Text("the"), new Text("hamster")});

    try (Scanner scanner = conn.createScanner(table, Authorizations.EMPTY)) {
      scanner.enableIsolation();
      scanner.addScanIterator(iterCfg);

      for (int i = 0; i < 100; i++) {
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("000A", iter.next().getKey().getColumnQualifierData().toString());
        Assert.assertFalse(iter.hasNext());
      }
    }
  }

  private void addDocument(BatchWriter bw, String docId, String... terms) throws MutationsRejectedException {
    Mutation m = new Mutation(String.format("%04d", docId.hashCode() % 10));
    for (String term : terms) {
      m.put(term, docId, "");
    }

    bw.addMutation(m);
  }
}
