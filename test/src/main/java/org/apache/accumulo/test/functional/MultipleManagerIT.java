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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MultipleManagerIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.MANAGER_PORTSEARCH, "true");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MAX_WAIT, "30s");
    cfg.getClusterServerConfiguration().setNumManagers(2);
  }

  @Test
  public void testMultipleManagers() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      List<String> managers = client.instanceOperations().getManagerLocations();
      assertEquals(2, managers.size());
      ClientContext cctx = (ClientContext) client;
      String primaryManager = cctx.getPrimaryManagerLocation();
      assertNotNull(primaryManager);
      assertTrue(managers.contains(primaryManager));

      client.tableOperations().create("t1");
      SortedSet<Text> splits = new TreeSet<Text>();
      IntStream.range(97, 122).forEach((i) -> splits.add(new Text(String.valueOf((char) i))));
      client.tableOperations().addSplits("t1", splits);
      ThriftClientTypes.MANAGER.executeVoid(cctx, c -> c.waitForBalance(TraceUtil.traceInfo()));

      client.tableOperations().create("t2");
      client.tableOperations().addSplits("t2", splits);
      ThriftClientTypes.MANAGER.executeVoid(cctx, c -> c.waitForBalance(TraceUtil.traceInfo()));
    }
  }

  public static List<TabletStats> getTabletStats(AccumuloClient c, String tableId)
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.TABLET_SERVER.execute((ClientContext) c, client -> client
        .getTabletStats(TraceUtil.traceInfo(), ((ClientContext) c).rpcCreds(), tableId));
  }

}
