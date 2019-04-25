/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class AccumuloOutputFormatTest {
  @Test
  public void testBWSettings() throws IOException {
    JobConf job = new JobConf();

    // make sure we aren't testing defaults
    final BatchWriterConfig bwDefaults = new BatchWriterConfig();
    assertNotEquals(7654321L, bwDefaults.getMaxLatency(TimeUnit.MILLISECONDS));
    assertNotEquals(9898989L, bwDefaults.getTimeout(TimeUnit.MILLISECONDS));
    assertNotEquals(42, bwDefaults.getMaxWriteThreads());
    assertNotEquals(1123581321L, bwDefaults.getMaxMemory());

    final BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxLatency(7654321L, TimeUnit.MILLISECONDS);
    bwConfig.setTimeout(9898989L, TimeUnit.MILLISECONDS);
    bwConfig.setMaxWriteThreads(42);
    bwConfig.setMaxMemory(1123581321L);

    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah")
        .batchWriterConfig(bwConfig).build();
    AccumuloOutputFormat.configure().clientProperties(cp).store(job);

    AccumuloOutputFormat myAOF = new AccumuloOutputFormat() {
      @Override
      public void checkOutputSpecs(FileSystem ignored, JobConf job) {
        BatchWriterConfig bwOpts =
            OutputConfigurator.getBatchWriterOptions(AccumuloOutputFormat.class, job);

        // passive check
        assertEquals(bwConfig.getMaxLatency(TimeUnit.MILLISECONDS),
            bwOpts.getMaxLatency(TimeUnit.MILLISECONDS));
        assertEquals(bwConfig.getTimeout(TimeUnit.MILLISECONDS),
            bwOpts.getTimeout(TimeUnit.MILLISECONDS));
        assertEquals(bwConfig.getMaxWriteThreads(), bwOpts.getMaxWriteThreads());
        assertEquals(bwConfig.getMaxMemory(), bwOpts.getMaxMemory());

        // explicit check
        assertEquals(7654321L, bwOpts.getMaxLatency(TimeUnit.MILLISECONDS));
        assertEquals(9898989L, bwOpts.getTimeout(TimeUnit.MILLISECONDS));
        assertEquals(42, bwOpts.getMaxWriteThreads());
        assertEquals(1123581321L, bwOpts.getMaxMemory());

      }
    };
    myAOF.checkOutputSpecs(null, job);
  }

  @Test
  public void testJobStoreException() throws Exception {
    JobConf job = new JobConf();

    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah").build();

    AccumuloOutputFormat.configure().clientProperties(cp);
    try {
      new AccumuloOutputFormat().checkOutputSpecs(null, job);
      fail("IllegalStateException should have been thrown.");
    } catch (IllegalStateException e) {}
  }

  @Test
  public void testCreateTables() {
    JobConf job = new JobConf();
    String tableName = "test_create_tables";

    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah").build();

    AccumuloOutputFormat.configure().clientProperties(cp).defaultTable(tableName).createTables(true)
        .store(job);

    assertEquals("createTables should be set to true", true,
        OutputConfigurator.canCreateTables(AccumuloOutputFormat.class, job));
  }

  @Test
  public void testSimulationMode() throws Exception {
    JobConf job = new JobConf();
    String tableName = "test_create_tables";
    Text tname = new Text(tableName);
    Mutation mutation = new Mutation();
    AccumuloOutputFormat myAOF = new AccumuloOutputFormat();

    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah").build();

    AccumuloOutputFormat.configure().clientProperties(cp).simulationMode(true).store(job);

    if (OutputConfigurator.getSimulationMode(AccumuloOutputFormat.class, job)) {
      // No output will occur since simulation mode is active. This line fails if simulation mode is
      // false
      myAOF.getRecordWriter(null, job, tableName, null).write(tname, mutation);
    } else
      fail("Simulation mode was not set");
  }

}
