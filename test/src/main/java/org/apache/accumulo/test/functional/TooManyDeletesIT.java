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

import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.tserver.compaction.strategies.TooManyDeletesCompactionStrategy;
import org.junit.Assert;
import org.junit.Test;

public class TooManyDeletesIT extends AccumuloClusterHarness {
  @Test
  public void tooManyDeletesCompactionStrategyIT() throws Exception {
    Connector c = getConnector();

    String table = getUniqueNames(1)[0];

    SummarizerConfiguration sc = SummarizerConfiguration.builder(DeletesSummarizer.class).build();

    // TODO open issue about programatic config of compaction strategies

    NewTableConfiguration ntc = new NewTableConfiguration().enableSummarization(sc);
    HashMap<String,String> props = new HashMap<>();
    props.put(Property.TABLE_COMPACTION_STRATEGY.getKey(), TooManyDeletesCompactionStrategy.class.getName());
    props.put(Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey() + TooManyDeletesCompactionStrategy.THRESHOLD_OPT, ".25");
    // ensure compaction does not happen because of the number of files
    props.put(Property.TABLE_MAJC_RATIO.getKey(), "10");
    ntc.setProperties(props);

    c.tableOperations().create(table, ntc);

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      for (int i = 0; i < 1000; i++) {
        Mutation m = new Mutation("row" + i);
        m.put("f", "q", "v" + i);
        bw.addMutation(m);
      }
    }

    List<Summary> summaries = c.tableOperations().summaries(table).flush(true).withConfiguration(sc).retrieve();
    Assert.assertEquals(1, summaries.size());

    Summary summary = summaries.get(0);

    Assert.assertEquals(1000l, (long) summary.getStatistics().get(DeletesSummarizer.TOTAL_STAT));
    Assert.assertEquals(0l, (long) summary.getStatistics().get(DeletesSummarizer.DELETES_STAT));

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      for (int i = 0; i < 100; i++) {
        Mutation m = new Mutation("row" + i);
        m.putDelete("f", "q");
        bw.addMutation(m);
      }
    }

    summaries = c.tableOperations().summaries(table).flush(true).withConfiguration(sc).retrieve();
    Assert.assertEquals(1, summaries.size());

    summary = summaries.get(0);

    Assert.assertEquals(1100l, (long) summary.getStatistics().get(DeletesSummarizer.TOTAL_STAT));
    Assert.assertEquals(100l, (long) summary.getStatistics().get(DeletesSummarizer.DELETES_STAT));

    try (BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig())) {
      for (int i = 100; i < 300; i++) {
        Mutation m = new Mutation("row" + i);
        m.putDelete("f", "q");
        bw.addMutation(m);
      }
    }

    // after a flush occurs Accumulo will check if a major compaction is needed. This check should call the compaction strategy, which should decide to compact
    // all files based on the number of deletes.
    c.tableOperations().flush(table, null, null, true);

    // wait for the compaction to happen
    while (true) {
      // the flush should cause
      summaries = c.tableOperations().summaries(table).flush(false).withConfiguration(sc).retrieve();
      Assert.assertEquals(1, summaries.size());

      summary = summaries.get(0);
      long total = summary.getStatistics().get(DeletesSummarizer.TOTAL_STAT);
      long deletes = summary.getStatistics().get(DeletesSummarizer.DELETES_STAT);

      if (total == 700 && deletes == 0) {
        // a compaction was triggered based on the number of deletes
        break;
      }

      UtilWaitThread.sleep(50);
    }
  }
}
