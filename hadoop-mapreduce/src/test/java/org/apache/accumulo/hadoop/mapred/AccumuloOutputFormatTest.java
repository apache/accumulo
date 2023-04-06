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
package org.apache.accumulo.hadoop.mapred;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;

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
    assertThrows(IllegalStateException.class,
        () -> new AccumuloOutputFormat().checkOutputSpecs(null, job));
  }

  @Test
  public void testCreateTables() {
    JobConf job = new JobConf();
    String tableName = "test_create_tables";

    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah").build();

    AccumuloOutputFormat.configure().clientProperties(cp).defaultTable(tableName).createTables(true)
        .store(job);

    assertTrue(OutputConfigurator.canCreateTables(AccumuloOutputFormat.class, job),
        "Should have been able to create table");
  }

  @Test
  public void testClientPropertiesPath() throws Exception {
    JobConf job = new JobConf();
    Properties cp = Accumulo.newClientProperties().to("test", "zk").as("blah", "blah").build();

    try {
      File file = File.createTempFile("accumulo-client", ".properties", null);
      file.deleteOnExit();

      FileWriter writer = new FileWriter(file, UTF_8);
      writer.write("auth.type=password\n");
      writer.write("instance.zookeepers=zk\n");
      writer.write("instance.name=test\n");
      writer.write("auth.principal=blah\n");
      writer.write("auth.token=blah");
      writer.close();

      AccumuloOutputFormat.configure().clientPropertiesPath(file.getAbsolutePath()).store(job);

      assertEquals(cp, OutputConfigurator.getClientProperties(AccumuloOutputFormat.class, job),
          "Properties from path does not match the expected values ");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
