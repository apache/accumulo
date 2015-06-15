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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.junit.Test;

/**
 * Prevent regression of ACCUMULO-3709.
 */
public class AccumuloOutputFormatIT extends ConfigurableMacBase {

  private static final String TABLE = "abc";

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "1");
    cfg.setNumTservers(1);
  }

  @Test
  public void testMapred() throws Exception {
    Connector connector = getConnector();
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
    AccumuloOutputFormat outputFormat = new AccumuloOutputFormat();
    AccumuloOutputFormat.setBatchWriterOptions(job, batchConfig);
    AccumuloOutputFormat.setZooKeeperInstance(job, cluster.getClientConfig());
    AccumuloOutputFormat.setConnectorInfo(job, "root", new PasswordToken(ROOT_PASSWORD));
    RecordWriter<Text,Mutation> writer = outputFormat.getRecordWriter(null, job, "Test", null);

    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation(new Text(String.format("%08d", i)));
        for (int j = 0; j < 3; j++) {
          m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));
        }
        writer.write(new Text(TABLE), m);
      }

    } catch (Exception e) {
      e.printStackTrace();
      // we don't want the exception to come from write
    }

    connector.securityOperations().revokeTablePermission("root", TABLE, TablePermission.WRITE);

    try {
      writer.close(null);
      fail("Did not throw exception");
    } catch (IOException ex) {
      log.info(ex.getMessage(), ex);
      assertTrue(ex.getCause() instanceof MutationsRejectedException);
    }
  }
}
