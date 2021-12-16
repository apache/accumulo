/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

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

  /**
   * Verify enableSampling returns
   */
  @Test
  public void testEnableSampling() {
    SamplerConfiguration sha1SamplerConfig = new SamplerConfiguration("com.mysampler");
    sha1SamplerConfig.setOptions(options);
    NewTableConfiguration ntcSample2 =
        new NewTableConfiguration().enableSampling(sha1SamplerConfig);
    assertEquals("com.mysampler", ntcSample2.getProperties().get("table.sampler"));
    assertEquals("5", ntcSample2.getProperties().get("table.sampler.opt.modulus"));
    assertEquals("murmur3_32", ntcSample2.getProperties().get("table.sampler.opt.hasher"));
  }

  /**
   * Verify enableSummarization returns SummarizerConfiguration with the expected class name(s).
   */
  @Test
  public void testEnableSummarization() {
    SummarizerConfiguration summarizerConfig1 = SummarizerConfiguration
        .builder("com.test.summarizer").setPropertyId("s1").addOption("opt1", "v1").build();
    NewTableConfiguration ntcSummarization1 =
        new NewTableConfiguration().enableSummarization(summarizerConfig1);
    assertEquals("v1", ntcSummarization1.getProperties().get("table.summarizer.s1.opt.opt1"));
    assertEquals("com.test.summarizer",
        ntcSummarization1.getProperties().get("table.summarizer.s1"));

    Class<? extends Summarizer> builderClass = FamilySummarizer.class;
    assertTrue(Summarizer.class.isAssignableFrom(builderClass));

    SummarizerConfiguration summarizerConfig2 = SummarizerConfiguration.builder(builderClass)
        .setPropertyId("s2").addOption("opt2", "v2").build();
    NewTableConfiguration ntcSummarization2 =
        new NewTableConfiguration().enableSummarization(summarizerConfig2);
    assertEquals("v2", ntcSummarization2.getProperties().get("table.summarizer.s2.opt.opt2"));
    assertEquals(builderClass.getName(),
        ntcSummarization2.getProperties().get("table.summarizer.s2"));

    NewTableConfiguration ntcSummarization3 =
        new NewTableConfiguration().enableSummarization(summarizerConfig1, summarizerConfig2);
    assertEquals("v1", ntcSummarization1.getProperties().get("table.summarizer.s1.opt.opt1"));
    assertEquals("v2", ntcSummarization2.getProperties().get("table.summarizer.s2.opt.opt2"));
    assertEquals("com.test.summarizer",
        ntcSummarization3.getProperties().get("table.summarizer.s1"));
    assertEquals(builderClass.getName(),
        ntcSummarization3.getProperties().get("table.summarizer.s2"));

  }
}
