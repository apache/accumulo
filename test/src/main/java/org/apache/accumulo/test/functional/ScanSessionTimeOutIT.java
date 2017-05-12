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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanSessionTimeOutIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ScanSessionTimeOutIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_SESSION_MAXIDLE.getKey(), getMaxIdleTimeString());
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private String sessionIdle = null;

  @Before
  public void reduceSessionIdle() throws Exception {
    InstanceOperations ops = getConnector().instanceOperations();
    sessionIdle = ops.getSystemConfiguration().get(Property.TSERV_SESSION_MAXIDLE.getKey());
    ops.setProperty(Property.TSERV_SESSION_MAXIDLE.getKey(), getMaxIdleTimeString());
    log.info("Waiting for existing session idle time to expire");
    Thread.sleep(ConfigurationTypeHelper.getTimeInMillis(sessionIdle));
    log.info("Finished waiting");
  }

  /**
   * Returns the max idle time as a string.
   *
   * @return new max idle time
   */
  protected String getMaxIdleTimeString() {
    return "3";
  }

  @After
  public void resetSessionIdle() throws Exception {
    if (null != sessionIdle) {
      getConnector().instanceOperations().setProperty(Property.TSERV_SESSION_MAXIDLE.getKey(), sessionIdle);
    }
  }

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < 100000; i++) {
      Mutation m = new Mutation(new Text(String.format("%08d", i)));
      for (int j = 0; j < 3; j++)
        m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));

      bw.addMutation(m);
    }

    bw.close();

    Scanner scanner = c.createScanner(tableName, new Authorizations());
    scanner.setBatchSize(1000);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    verify(iter, 0, 200);

    // sleep three times the session timeout
    sleepUninterruptibly(9, TimeUnit.SECONDS);

    verify(iter, 200, 100000);

  }

  protected void verify(Iterator<Entry<Key,Value>> iter, int start, int stop) throws Exception {
    for (int i = start; i < stop; i++) {

      Text er = new Text(String.format("%08d", i));

      for (int j = 0; j < 3; j++) {
        Entry<Key,Value> entry = iter.next();

        if (!entry.getKey().getRow().equals(er)) {
          throw new Exception("row " + entry.getKey().getRow() + " != " + er);
        }

        if (!entry.getKey().getColumnFamily().equals(new Text("cf1"))) {
          throw new Exception("cf " + entry.getKey().getColumnFamily() + " != cf1");
        }

        if (!entry.getKey().getColumnQualifier().equals(new Text("cq" + j))) {
          throw new Exception("cq " + entry.getKey().getColumnQualifier() + " != cq" + j);
        }

        if (!entry.getValue().toString().equals("" + i + "_" + j)) {
          throw new Exception("value " + entry.getValue() + " != " + i + "_" + j);
        }

      }
    }

  }

}
