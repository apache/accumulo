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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.client.mapreduce.lib.impl.FileOutputConfigurator;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class AccumuloFileOutputFormatTest {

  @Test
  public void validateConfiguration() throws IOException, InterruptedException {

    int a = 7;
    long b = 300l;
    long c = 50l;
    long d = 10l;
    String e = "snappy";
    SamplerConfiguration samplerConfig = new SamplerConfiguration(RowSampler.class.getName());
    samplerConfig.addOption("hasher", "murmur3_32");
    samplerConfig.addOption("modulus", "109");

    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(VisibilitySummarizer.class).addOption(CountingSummarizer.MAX_COUNTERS_OPT, 2048).build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(FamilySummarizer.class).addOption(CountingSummarizer.MAX_COUNTERS_OPT, 256).build();

    Job job1 = Job.getInstance();
    AccumuloFileOutputFormat.setReplication(job1, a);
    AccumuloFileOutputFormat.setFileBlockSize(job1, b);
    AccumuloFileOutputFormat.setDataBlockSize(job1, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job1, d);
    AccumuloFileOutputFormat.setCompressionType(job1, e);
    AccumuloFileOutputFormat.setSampler(job1, samplerConfig);
    AccumuloFileOutputFormat.setSummarizers(job1, sc1, sc2);

    AccumuloConfiguration acuconf = FileOutputConfigurator.getAccumuloConfiguration(AccumuloFileOutputFormat.class, job1.getConfiguration());

    assertEquals(7, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(300l, acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(50l, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(10l, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("snappy", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    assertEquals(new SamplerConfigurationImpl(samplerConfig), SamplerConfigurationImpl.newSamplerConfig(acuconf));

    Collection<SummarizerConfiguration> summarizerConfigs = SummarizerConfiguration.fromTableProperties(acuconf);
    assertEquals(2, summarizerConfigs.size());
    assertTrue(summarizerConfigs.contains(sc1));
    assertTrue(summarizerConfigs.contains(sc2));

    a = 17;
    b = 1300l;
    c = 150l;
    d = 110l;
    e = "lzo";
    samplerConfig = new SamplerConfiguration(RowSampler.class.getName());
    samplerConfig.addOption("hasher", "md5");
    samplerConfig.addOption("modulus", "100003");

    Job job2 = Job.getInstance();
    AccumuloFileOutputFormat.setReplication(job2, a);
    AccumuloFileOutputFormat.setFileBlockSize(job2, b);
    AccumuloFileOutputFormat.setDataBlockSize(job2, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job2, d);
    AccumuloFileOutputFormat.setCompressionType(job2, e);
    AccumuloFileOutputFormat.setSampler(job2, samplerConfig);

    acuconf = FileOutputConfigurator.getAccumuloConfiguration(AccumuloFileOutputFormat.class, job2.getConfiguration());

    assertEquals(17, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(1300l, acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(150l, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(110l, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("lzo", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    assertEquals(new SamplerConfigurationImpl(samplerConfig), SamplerConfigurationImpl.newSamplerConfig(acuconf));

    summarizerConfigs = SummarizerConfiguration.fromTableProperties(acuconf);
    assertEquals(0, summarizerConfigs.size());

  }
}
