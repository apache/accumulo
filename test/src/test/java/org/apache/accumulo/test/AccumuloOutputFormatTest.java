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
package org.apache.accumulo.test;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.util.OutputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Maps;

/**
 * Exists because mock instance doesn't produce this error when dynamically changing the table permissions.
 */
public class AccumuloOutputFormatTest {

  private static final String TABLE = "abc";
  public static TemporaryFolder folder = new TemporaryFolder();
  private MiniAccumuloCluster accumulo;
  private String secret = "secret";

  @Before
  public void setUp() throws Exception {
    folder.create();
    MiniAccumuloConfig config = new MiniAccumuloConfig(folder.getRoot(), secret);

    Map<String,String> configMap = Maps.newHashMap();
    configMap.put(Property.TSERV_SESSION_MAXIDLE.toString(), "1");
    config.setSiteConfig(configMap);
    config.setNumTservers(1);
    accumulo = new MiniAccumuloCluster(config);

    accumulo.start();
  }

  @After
  public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }

  @Test(expected = IOException.class)
  public void testMapred() throws Exception {

    ZooKeeperInstance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(secret));
    // create a table and put some data in it
    connector.tableOperations().create(TABLE);

    JobConf job = new JobConf();
    BatchWriterConfig batchConfig = new BatchWriterConfig();
    // no flushes!!!!!
    batchConfig.setMaxLatency(0, TimeUnit.MILLISECONDS);
    // use a single thread to ensure our update session times out
    batchConfig.setMaxWriteThreads(1);
    // set the max memory so that we ensure we don't flush on the write.
    batchConfig.setMaxMemory(Long.MAX_VALUE);
    OutputConfigurator.setBatchWriterOptions(AccumuloOutputFormat.class, job, batchConfig);
    AccumuloOutputFormat outputFormat = new AccumuloOutputFormat();
    AccumuloOutputFormat.setZooKeeperInstance(job, accumulo.getInstanceName(), accumulo.getZooKeepers());
    AccumuloOutputFormat.setConnectorInfo(job, "root", new PasswordToken(secret));
    RecordWriter<Text,Mutation> writer = outputFormat.getRecordWriter(null, job, "Test", null);

    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation(new Text(String.format("%08d", i)));
        for (int j = 0; j < 3; j++)
          m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));

        writer.write(new Text(TABLE), m);
      }
    } catch (Exception e) {
      e.printStackTrace();
      // we don't want the exception to come from write
    }
    connector.securityOperations().revokeTablePermission("root", TABLE, TablePermission.WRITE);
    writer.close(null);
    connector.tableOperations().delete(TABLE);
  }

  // this isn't really necessary, but let's cover both
  @Test(expected = IOException.class)
  public void testMapreduce() throws Exception {

    ZooKeeperInstance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(secret));
    // create a table and put some data in it
    connector.tableOperations().create(TABLE);

    JobConf job = new JobConf();
    BatchWriterConfig batchConfig = new BatchWriterConfig();
    // no flushes!!!!!
    batchConfig.setMaxLatency(0, TimeUnit.MILLISECONDS);
    // use a single thread to ensure our update session times out
    batchConfig.setMaxWriteThreads(1);
    // set the max memory so that we ensure we don't flush on the write.
    batchConfig.setMaxMemory(Long.MAX_VALUE);
    OutputConfigurator.setBatchWriterOptions(org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.class, job, batchConfig);
    org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat outputFormat = new org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat();
    org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setZooKeeperInstance(job, accumulo.getInstanceName(), accumulo.getZooKeepers());
    Job stubbedJob = new Job(job);
    org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setConnectorInfo(stubbedJob, "root", new PasswordToken(secret));
    TaskAttemptContext context = new TaskAttemptContext(stubbedJob.getConfiguration(), new TaskAttemptID());
    org.apache.hadoop.mapreduce.RecordWriter<Text,Mutation> writer = outputFormat.getRecordWriter(context);

    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation(new Text(String.format("%08d", i)));
        for (int j = 0; j < 3; j++)
          m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));

        writer.write(new Text(TABLE), m);
      }
    } catch (Exception e) {
      e.printStackTrace();
      // we don't want the exception to come from write
    }
    connector.securityOperations().revokeTablePermission("root", TABLE, TablePermission.WRITE);
    writer.close(null);
    connector.tableOperations().delete(TABLE);
  }
}
