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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

public class KeyValueEqualityIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testEquality() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      final String[] tables = getUniqueNames(2);
      final String table1 = tables[0], table2 = tables[1];
      final TableOperations tops = client.tableOperations();
      tops.create(table1);
      tops.create(table2);

      try (BatchWriter bw1 = client.createBatchWriter(table1);
          BatchWriter bw2 = client.createBatchWriter(table2)) {

        for (int row = 0; row < 100; row++) {
          Mutation m = new Mutation(Integer.toString(row));
          for (int col = 0; col < 10; col++) {
            m.put(Integer.toString(col), "", System.currentTimeMillis(), Integer.toString(col * 2));
          }
          bw1.addMutation(m);
          bw2.addMutation(m);
        }
      }

      Iterator<Entry<Key,Value>> t1 = client.createScanner(table1, Authorizations.EMPTY).iterator(),
          t2 = client.createScanner(table2, Authorizations.EMPTY).iterator();
      while (t1.hasNext() && t2.hasNext()) {
        // KeyValue, the implementation of Entry<Key,Value>, should support equality and hashCode
        // properly
        Entry<Key,Value> e1 = t1.next(), e2 = t2.next();
        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
      }
      assertFalse(t1.hasNext(), "table1 had more data to read");
      assertFalse(t2.hasNext(), "table2 had more data to read");
    }
  }
}
