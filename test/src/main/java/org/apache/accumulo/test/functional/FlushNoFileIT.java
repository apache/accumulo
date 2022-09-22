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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

/**
 * Tests that Accumulo will flush but not create a file that has 0 entries.
 */
public class FlushNoFileIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting iteratorSetting = new IteratorSetting(20, NullIterator.class);
      ntc.attachIterator(iteratorSetting, EnumSet.of(IteratorUtil.IteratorScope.minc));
      ntc.withSplits(new TreeSet<>(Set.of(new Text("a"), new Text("s"))));

      c.tableOperations().create(tableName, ntc);
      TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r1"));
        m.put("acf", tableName, "1");
        bw.addMutation(m);
      }

      FunctionalTestUtils.checkRFiles(c, tableName, 3, 3, 0, 0);

      c.tableOperations().flush(tableName, null, null, true);

      FunctionalTestUtils.checkRFiles(c, tableName, 3, 3, 0, 0);

      long flushId = FunctionalTestUtils.checkFlushId((ClientContext) c, tableId, 0);

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r2"));
        m.put("acf", tableName, "1");
        bw.addMutation(m);
      }

      c.tableOperations().flush(tableName, null, null, true);

      FunctionalTestUtils.checkRFiles(c, tableName, 3, 3, 0, 0);

      long secondFlushId = FunctionalTestUtils.checkFlushId((ClientContext) c, tableId, flushId);
      assertTrue(secondFlushId > flushId, "Flush ID did not change");

      try (Scanner scanner = c.createScanner(tableName)) {
        assertEquals(0, Iterables.size(scanner), "Expected 0 Entries in table");
      }
    }
  }

  public static class NullIterator implements SortedKeyValueIterator<Key,Value> {

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {}

    @Override
    public boolean hasTop() {
      return false;
    }

    @Override
    public void next() throws IOException {}

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {}

    @Override
    public Key getTopKey() {
      return null;
    }

    @Override
    public Value getTopValue() {
      return null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return null;
    }
  }
}
