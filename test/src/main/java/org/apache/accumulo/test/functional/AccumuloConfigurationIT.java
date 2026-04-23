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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccumuloConfigurationIT extends SharedMiniClusterBase {

  private static final String fakeProperty = "general.custom.fake.property";

  private static class ConfigurationCallback implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(fakeProperty, "1");
    }

  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new ConfigurationCallback());
  }

  @AfterAll
  public static void afterTests() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testInvalidation() throws Exception {

    final ServerContext ctx = getCluster().getServerContext();
    String initialThreads = ctx.getConfiguration().get(fakeProperty);

    Timer timer = null;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      timer = Timer.startNew();
      c.instanceOperations().setProperty(fakeProperty, "4");
    }

    ctx.getConfiguration().invalidateCache();

    int oldValueReturned = 0;
    while (ctx.getConfiguration().get(fakeProperty).equals(initialThreads)) {
      oldValueReturned++;
      Thread.sleep(25);
    }
    System.out.println("Configuration returned old value " + oldValueReturned + " times and took "
        + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");

    assertEquals("4", ctx.getConfiguration().get(fakeProperty));
    assertEquals(0, oldValueReturned);

  }

}
