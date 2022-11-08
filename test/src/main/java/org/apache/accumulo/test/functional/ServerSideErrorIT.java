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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ServerSideErrorIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      IteratorSetting is = new IteratorSetting(5, "Bad Aggregator", BadCombiner.class);
      Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("acf")));
      c.tableOperations().attachIterator(tableName, is);

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r1"));
        m.put("acf", "foo", new Value(new byte[] {'1'}));
        bw.addMutation(m);
      }

      // try to scan table
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        Iterator<Entry<Key,Value>> iterator = scanner.iterator();
        assertThrows(RuntimeException.class, iterator::hasNext);
      }

      // try to batch scan the table
      try (BatchScanner bs = c.createBatchScanner(tableName, Authorizations.EMPTY, 2)) {
        bs.setRanges(Collections.singleton(new Range()));
        Iterator<Entry<Key,Value>> iterator = bs.iterator();
        assertThrows(RuntimeException.class, iterator::hasNext);
      }

      // remove the bad agg so accumulo can shutdown
      TableOperations to = c.tableOperations();
      Iterable<Entry<String,String>> tableProps = to.getProperties(tableName);
      to.modifyProperties(tableName, properties -> {
        for (Entry<String,String> e : tableProps) {
          properties.remove(e.getKey());
        }
      });

      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

      // should be able to scan now
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
        // set a nonexistent iterator, should cause scan to fail on server side
        scanner.addScanIterator(new IteratorSetting(100, "bogus", "com.bogus.iterator"));
        Iterator<Entry<Key,Value>> iterator = scanner.iterator();
        assertThrows(RuntimeException.class, iterator::hasNext);
      }
    }
  }

}
