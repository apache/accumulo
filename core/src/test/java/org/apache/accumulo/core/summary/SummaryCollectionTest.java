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

package org.apache.accumulo.core.summary;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.Summary.FileStatistics;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.summary.SummaryCollection.FileSummary;
import org.junit.Assert;
import org.junit.Test;

public class SummaryCollectionTest {
  @Test
  public void testDeleted() {
    SummarizerConfiguration conf = SummarizerConfiguration.builder(FamilySummarizer.class).build();

    HashMap<String,Long> stats = new HashMap<>();
    stats.put("c:foo", 9L);
    FileSummary fs1 = new FileSummary(conf, stats, false);
    SummaryCollection sc1 = new SummaryCollection(Collections.singleton(fs1));

    stats = new HashMap<>();
    stats.put("c:foo", 5L);
    stats.put("c:bar", 3L);
    FileSummary fs2 = new FileSummary(conf, stats, true);
    SummaryCollection sc2 = new SummaryCollection(Collections.singleton(fs2));

    SummaryCollection sc3 = new SummaryCollection(Collections.emptyList());

    SummaryCollection sc4 = new SummaryCollection(Collections.emptyList(), true);

    SummarizerFactory factory = new SummarizerFactory();
    SummaryCollection mergeSc = new SummaryCollection();
    for (SummaryCollection sc : Arrays.asList(sc1, sc2, sc3, sc4, sc4)) {
      mergeSc.merge(sc, factory);
    }

    for (SummaryCollection sc : Arrays.asList(mergeSc, new SummaryCollection(mergeSc.toThrift()))) {
      List<Summary> summaries = sc.getSummaries();
      Assert.assertEquals(1, summaries.size());
      Summary summary = summaries.get(0);
      FileStatistics filestats = summary.getFileStatistics();
      Assert.assertEquals(5, filestats.getTotal());
      Assert.assertEquals(1, filestats.getExtra());
      Assert.assertEquals(0, filestats.getLarge());
      Assert.assertEquals(1, filestats.getMissing());
      Assert.assertEquals(2, filestats.getDeleted());
      Assert.assertEquals(4, filestats.getInaccurate());
    }
  }
}
