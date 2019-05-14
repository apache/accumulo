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
package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NewTableConfigurationTest {

  private SortedSet<Text> splits;
  private Map<String,String> options;

  @Before
  public void setup() {
    populateSplits();
    populateOptions();
  }

  public void populateSplits() {
    splits = new TreeSet<>();
    splits.add(new Text("ccccc"));
    splits.add(new Text("aaaaa"));
    splits.add(new Text("ddddd"));
    splits.add(new Text("abcde"));
    splits.add(new Text("bbbbb"));
  }

  /**
   * Verify the withSplits/getSplits methods do as expected.
   *
   * The withSplits() takes a SortedSet as its input. Verify that the set orders the data even if
   * input non-ordered.
   *
   * The getSplits should return a SortedSet. Test verifies set performs ordering and the input set
   * and output set are equal.
   */
  @Test
  public void testWithAndGetSplits() {
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
    Collection<Text> ntcSplits = ntc.getSplits();
    Iterator<Text> splitIt = splits.iterator();
    Iterator<Text> ntcIt = ntcSplits.iterator();
    while (splitIt.hasNext() && ntcIt.hasNext()) {
      assertEquals(splitIt.next(), ntcIt.next());
    }
    // verify splits is in sorted order
    Iterator<Text> it = splits.iterator();
    Text current = new Text("");
    while (it.hasNext()) {
      Text nxt = it.next();
      assertTrue(current.toString().compareTo(nxt.toString()) < 0);
      current = nxt;
    }
    // verify ntcSplits is in sorted order
    Iterator<Text> it2 = ntcSplits.iterator();
    current = new Text("");
    while (it2.hasNext()) {
      Text nxt = it2.next();
      assertTrue(current.toString().compareTo(nxt.toString()) < 0);
      current = nxt;
    }

    NewTableConfiguration ntc2 = new NewTableConfiguration();
    Collection<Text> splits = ntc2.getSplits();
    assertTrue(splits.isEmpty());

  }

  /**
   * Verify that createOffline option
   */
  @Test
  public void testCreateOffline() {
    NewTableConfiguration ntcOffline = new NewTableConfiguration().createOffline();
    assertTrue(ntcOffline.getInitialTableState() == InitialTableState.OFFLINE);
    NewTableConfiguration ntcOnline = new NewTableConfiguration();
    assertTrue(ntcOnline.getInitialTableState() == InitialTableState.ONLINE);
  }

  public void populateOptions() {
    options = new HashMap<>();
    options.put("hasher", "murmur3_32");
    options.put("modulus", "5");
  }

  public void populateInvalidOptions() {}

  /**
   * Verify enableSampling returns
   */
  @Test
  public void testEnableSampling() {
    SamplerConfiguration samplerConfig = new SamplerConfiguration("test");
    NewTableConfiguration ntcSample1 = new NewTableConfiguration().enableSampling(samplerConfig);
    assertTrue(ntcSample1.getProperties().containsValue("test"));

    RowSampler rowSampler = new RowSampler();
    SamplerConfiguration sha1SamplerConfig = new SamplerConfiguration(rowSampler.getClass());
    sha1SamplerConfig.setOptions(options);
    rowSampler.init(sha1SamplerConfig);
    NewTableConfiguration ntcSample2 =
        new NewTableConfiguration().enableSampling(sha1SamplerConfig);
    assertTrue(ntcSample2.getProperties().containsValue("murmur3_32"));
  }

  /**
   * Verify enableSummarization returns SummarizerConfiguration with the expected class name.
   */
  @Test
  public void testEnableSummarization() {
    SummarizerConfiguration summarizerConfig1 = SummarizerConfiguration.builder("test").build();
    NewTableConfiguration ntcSummarization1 =
            new NewTableConfiguration().enableSummarization(summarizerConfig1);

    ArrayList<String> summarizerValues = new ArrayList<String>();
    for (Map.Entry<String,String> e : ntcSummarization1.getProperties().entrySet()) {
      if (e.getKey().contains("table.summarizer")) {
        summarizerValues.add(e.getValue());
      }
    }
    assertTrue(summarizerValues.get(0) instanceof String);

    Class<? extends Summarizer> builderClass = FamilySummarizer.class;
    assertTrue(Summarizer.class.isAssignableFrom(builderClass));

    summarizerValues.clear();
    SummarizerConfiguration summarizerConfig2 =
        SummarizerConfiguration.builder(builderClass).build();
    NewTableConfiguration ntcSummarization2 =
            new NewTableConfiguration().enableSummarization(summarizerConfig2);

    for (Map.Entry<String,String> e : ntcSummarization2.getProperties().entrySet()) {
      if (e.getKey().contains("table.summarizer")) {
        summarizerValues.add(e.getValue());
      }
    }
    assertTrue(summarizerValues.get(0).equals(builderClass.getName()));

    NewTableConfiguration ntcSummarization3 =
            new NewTableConfiguration().enableSummarization(summarizerConfig1, summarizerConfig2);

    summarizerValues.clear();
    for (Map.Entry<String,String> e : ntcSummarization3.getProperties().entrySet()) {
      if (e.getKey().contains("table.summarizer")) {
        summarizerValues.add(e.getValue());
      }
    }
    assertEquals(summarizerValues.size(), 2);
  }
}
