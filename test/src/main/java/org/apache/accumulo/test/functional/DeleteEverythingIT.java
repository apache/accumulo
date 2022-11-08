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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleteEverythingIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    cfg.setSiteConfig(siteConfig);
  }

  private String majcDelay;

  @BeforeEach
  public void updateMajcDelay() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      majcDelay =
          c.instanceOperations().getSystemConfiguration().get(Property.TSERV_MAJC_DELAY.getKey());
      c.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1s");
      if (getClusterType() == ClusterType.STANDALONE) {
        // Gotta wait for the cluster to get out of the default sleep value
        Thread.sleep(ConfigurationTypeHelper.getTimeInMillis(majcDelay));
      }
    }
  }

  @AfterEach
  public void resetMajcDelay() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
    }
  }

  @Test
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      BatchWriter bw = c.createBatchWriter(tableName);
      Mutation m = new Mutation(new Text("foo"));
      m.put("bar", "1910", "5");
      bw.addMutation(m);
      bw.flush();

      c.tableOperations().flush(tableName, null, null, true);

      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 1, 1);

      m = new Mutation(new Text("foo"));
      m.putDelete(new Text("bar"), new Text("1910"));
      bw.addMutation(m);
      bw.flush();

      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());

        assertTrue(scanner.stream().findAny().isEmpty());
        c.tableOperations().flush(tableName, null, null, true);

        c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
        sleepUninterruptibly(4, TimeUnit.SECONDS);

        FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

        bw.close();

        assertTrue(scanner.stream().findAny().isEmpty());

      }
    }
  }
}
