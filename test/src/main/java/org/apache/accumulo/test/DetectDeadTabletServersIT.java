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
package org.apache.accumulo.test;

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class DetectDeadTabletServersIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      log.info("verifying that everything is up");
      try (Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      ManagerMonitorInfo stats = getStats(c);
      assertEquals(2, stats.tServerInfo.size());
      assertEquals(0, stats.badTServers.size());
      assertEquals(0, stats.deadTabletServers.size());
      log.info("Killing a tablet server");
      getCluster().killProcess(TABLET_SERVER,
          getCluster().getProcesses().get(TABLET_SERVER).iterator().next());

      while (true) {
        stats = getStats(c);
        if (stats.tServerInfo.size() != 2) {
          break;
        }
        UtilWaitThread.sleep(500);
      }
      assertEquals(1, stats.tServerInfo.size());
      assertEquals(1, stats.badTServers.size() + stats.deadTabletServers.size());
      while (true) {
        stats = getStats(c);
        if (!stats.deadTabletServers.isEmpty()) {
          break;
        }
        UtilWaitThread.sleep(500);
      }
      assertEquals(1, stats.tServerInfo.size());
      assertEquals(0, stats.badTServers.size());
      assertEquals(1, stats.deadTabletServers.size());
    }
  }

  private ManagerMonitorInfo getStats(AccumuloClient c) throws Exception {
    final ClientContext context = (ClientContext) c;
    return ThriftClientTypes.MANAGER.execute(context,
        client -> client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds()));
  }

}
