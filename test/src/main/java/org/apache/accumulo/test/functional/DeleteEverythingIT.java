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
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class DeleteEverythingIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private String majcDelay;

  @Before
  public void updateMajcDelay() throws Exception {
    Connector c = getConnector();
    majcDelay = c.instanceOperations().getSystemConfiguration().get(Property.TSERV_MAJC_DELAY.getKey());
    c.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    if (getClusterType() == ClusterType.STANDALONE) {
      // Gotta wait for the cluster to get out of the default sleep value
      Thread.sleep(ConfigurationTypeHelper.getTimeInMillis(majcDelay));
    }
  }

  @After
  public void resetMajcDelay() throws Exception {
    Connector c = getConnector();
    c.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
  }

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation(new Text("foo"));
    m.put(new Text("bar"), new Text("1910"), new Value("5".getBytes(UTF_8)));
    bw.addMutation(m);
    bw.flush();

    getConnector().tableOperations().flush(tableName, null, null, true);

    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 1, 1);

    m = new Mutation(new Text("foo"));
    m.putDelete(new Text("bar"), new Text("1910"));
    bw.addMutation(m);
    bw.flush();

    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new Range());
    int count = Iterators.size(scanner.iterator());
    assertEquals("count == " + count, 0, count);
    getConnector().tableOperations().flush(tableName, null, null, true);

    getConnector().tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
    sleepUninterruptibly(4, TimeUnit.SECONDS);

    FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

    bw.close();

    count = Iterables.size(scanner);

    if (count != 0)
      throw new Exception("count == " + count);
  }
}
