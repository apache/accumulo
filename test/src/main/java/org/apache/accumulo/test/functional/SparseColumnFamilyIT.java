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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * This test recreates issue ACCUMULO-516. Until that issue is fixed this test should time out.
 */
public class SparseColumnFamilyIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void sparceColumnFamily() throws Exception {
    String scftt = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(scftt);

    BatchWriter bw = c.createBatchWriter(scftt, new BatchWriterConfig());

    // create file in the tablet that has mostly column family 0, with a few entries for column family 1

    bw.addMutation(nm(0, 1, 0));
    for (int i = 1; i < 99999; i++) {
      bw.addMutation(nm(i * 2, 0, i));
    }
    bw.addMutation(nm(99999 * 2, 1, 99999));
    bw.flush();

    c.tableOperations().flush(scftt, null, null, true);

    // create a file that has column family 1 and 0 interleaved
    for (int i = 0; i < 100000; i++) {
      bw.addMutation(nm(i * 2 + 1, i % 2 == 0 ? 0 : 1, i));
    }
    bw.close();

    c.tableOperations().flush(scftt, null, null, true);

    try (Scanner scanner = c.createScanner(scftt, Authorizations.EMPTY)) {

      for (int i = 0; i < 200; i++) {

        // every time we search for column family 1, it will scan the entire file
        // that has mostly column family 0 until the bug is fixed
        scanner.setRange(new Range(String.format("%06d", i), null));
        scanner.clearColumns();
        scanner.setBatchSize(3);
        scanner.fetchColumnFamily(new Text(String.format("%03d", 1)));

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        if (iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          if (!"001".equals(entry.getKey().getColumnFamilyData().toString())) {
            throw new Exception();
          }
        }
      }
    }
  }

  private Mutation nm(int row, int cf, int val) {
    Mutation m = new Mutation(String.format("%06d", row));
    m.put(String.format("%03d", cf), "", "" + val);
    return m;
  }
}
