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
package org.apache.accumulo.core.client.mapred;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

/**
 * @deprecated since 2.0.0
 */
@Deprecated
public class AccumuloFileOutputFormatTest {

  @Test
  public void validateConfiguration() {

    int a = 7;
    long b = 300L;
    long c = 50L;
    long d = 10L;
    String e = "snappy";
    SamplerConfiguration samplerConfig = new SamplerConfiguration(RowSampler.class.getName());
    samplerConfig.addOption("hasher", "murmur3_32");
    samplerConfig.addOption("modulus", "109");

    JobConf job = new JobConf();
    AccumuloFileOutputFormat.setReplication(job, a);
    AccumuloFileOutputFormat.setFileBlockSize(job, b);
    AccumuloFileOutputFormat.setDataBlockSize(job, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job, d);
    AccumuloFileOutputFormat.setCompressionType(job, e);
    AccumuloFileOutputFormat.setSampler(job, samplerConfig);

    AccumuloConfiguration acuconf =
        org.apache.accumulo.core.clientImpl.mapreduce.lib.FileOutputConfigurator
            .getAccumuloConfiguration(AccumuloFileOutputFormat.class, job);

    assertEquals(7, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(300L, acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(50L, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(10L, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("snappy", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    assertEquals(new SamplerConfigurationImpl(samplerConfig),
        SamplerConfigurationImpl.newSamplerConfig(acuconf));

    a = 17;
    b = 1300L;
    c = 150L;
    d = 110L;
    e = "lzo";
    samplerConfig = new SamplerConfiguration(RowSampler.class.getName());
    samplerConfig.addOption("hasher", "md5");
    samplerConfig.addOption("modulus", "100003");

    job = new JobConf();
    AccumuloFileOutputFormat.setReplication(job, a);
    AccumuloFileOutputFormat.setFileBlockSize(job, b);
    AccumuloFileOutputFormat.setDataBlockSize(job, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job, d);
    AccumuloFileOutputFormat.setCompressionType(job, e);
    AccumuloFileOutputFormat.setSampler(job, samplerConfig);

    acuconf = org.apache.accumulo.core.clientImpl.mapreduce.lib.FileOutputConfigurator
        .getAccumuloConfiguration(AccumuloFileOutputFormat.class, job);

    assertEquals(17, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(1300L, acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(150L, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(110L, acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("lzo", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    assertEquals(new SamplerConfigurationImpl(samplerConfig),
        SamplerConfigurationImpl.newSamplerConfig(acuconf));

  }
}
