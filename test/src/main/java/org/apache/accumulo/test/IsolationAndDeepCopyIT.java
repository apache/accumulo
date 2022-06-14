/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class IsolationAndDeepCopyIT extends AccumuloClusterHarness {

  @Test
  public void testBugFix() throws Exception {
    // test bug fox for ACCUMULO-3977

    String table = super.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (BatchWriter bw = client.createBatchWriter(table)) {
        addDocument(bw, "000A", "dog", "cat", "hamster", "iguana", "the");
        addDocument(bw, "000B", "java", "perl", "C++", "pascal", "the");
        addDocument(bw, "000C", "chrome", "firefox", "safari", "opera", "the");
        addDocument(bw, "000D", "logarithmic", "quadratic", "linear", "exponential", "the");
      }

      // its a bug when using rfiles, so flush
      client.tableOperations().flush(table, null, null, true);

      IteratorSetting iterCfg =
          new IteratorSetting(30, "ayeaye", IntersectingIterator.class.getName());
      IntersectingIterator.setColumnFamilies(iterCfg,
          new Text[] {new Text("the"), new Text("hamster")});

      try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
        scanner.enableIsolation();
        scanner.addScanIterator(iterCfg);

        for (int i = 0; i < 100; i++) {
          String actual = getOnlyElement(scanner).getKey().getColumnQualifierData().toString();
          assertEquals("000A", actual);
        }
      }
    }
  }

  private void addDocument(BatchWriter bw, String docId, String... terms)
      throws MutationsRejectedException {
    Mutation m = new Mutation(String.format("%04d", docId.hashCode() % 10));
    for (String term : terms) {
      m.put(term, docId, "");
    }

    bw.addMutation(m);
  }
}
