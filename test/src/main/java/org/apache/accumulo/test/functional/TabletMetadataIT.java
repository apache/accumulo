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
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests features of the Ample TabletMetadata class that can't be tested in TabletMetadataTest
 */
public class TabletMetadataIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(TabletMetadataIT.class);
  private static final int NUM_TSERVERS = 3;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setNumTservers(NUM_TSERVERS);
  }

  @Test
  public void getLiveTServersTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      while (c.instanceOperations().getTabletServers().size() != NUM_TSERVERS) {
        log.info("Waiting for tservers to start up...");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      Set<TServerInstance> servers = TabletMetadata.getLiveTServers((ClientContext) c);
      assertEquals(NUM_TSERVERS, servers.size());

      // kill a tserver and see if its gone from the list
      getCluster().killProcess(TABLET_SERVER,
          getCluster().getProcesses().get(TABLET_SERVER).iterator().next());

      while (c.instanceOperations().getTabletServers().size() == NUM_TSERVERS) {
        log.info("Waiting for a tserver to die...");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      servers = TabletMetadata.getLiveTServers((ClientContext) c);
      assertEquals(NUM_TSERVERS - 1, servers.size());
    }
  }
}
