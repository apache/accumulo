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

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Tests iterator class hierarchy bug. The failure condition of this test is to hang on the flush
 * due to a class cast exception on the tserver. See https://github.com/apache/accumulo/issues/2341
 */
public class IteratorMincClassCastBugIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // this bug only shows up when not using native maps
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, "false");
    cfg.setNumTservers(1);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];

      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,Set<Text>> groups = new HashMap<>();
      groups.put("g1", Set.of(new Text("even")));
      groups.put("g2", Set.of(new Text("odd")));
      ntc.setLocalityGroups(groups);

      IteratorSetting iteratorSetting = new IteratorSetting(20, ClasCastIterator.class);
      ntc.attachIterator(iteratorSetting, EnumSet.of(IteratorUtil.IteratorScope.minc));

      c.tableOperations().create(tableName, ntc);

      try (BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig())) {
        Mutation m1 = new Mutation(new Text("r1"));
        m1.put(new Text("odd"), new Text(), new Value("1"));
        bw.addMutation(m1);
        Mutation m2 = new Mutation(new Text("r2"));
        m2.put(new Text("even"), new Text(), new Value("2"));
        bw.addMutation(m2);
      }

      // class cast exception will happen in tserver and hang on flush
      c.tableOperations().flush(tableName, null, null, true);
    }
  }

  /**
   * To simulate the bug we only need direct implementation of SKVI that does a deepCopy on source.
   */
  public static class ClasCastIterator implements SortedKeyValueIterator<Key,Value> {
    SortedKeyValueIterator<Key,Value> ref;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      ref = source.deepCopy(env);
    }

    @Override
    public boolean hasTop() {
      return ref.hasTop();
    }

    @Override
    public void next() throws IOException {
      ref.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      ref.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
      return ref.getTopKey();
    }

    @Override
    public Value getTopValue() {
      return ref.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }
  }
}
