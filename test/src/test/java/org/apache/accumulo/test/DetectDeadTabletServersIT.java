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

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class DetectDeadTabletServersIT extends ConfigurableMacIT {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "3s");
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    log.info("verifying that everything is up");
    Iterators.size(c.createScanner(MetadataTable.NAME, Authorizations.EMPTY).iterator());

    MasterMonitorInfo stats = getStats(c);
    assertEquals(2, stats.tServerInfo.size());
    assertEquals(0, stats.badTServers.size());
    assertEquals(0, stats.deadTabletServers.size());
    log.info("Killing a tablet server");
    getCluster().killProcess(TABLET_SERVER, getCluster().getProcesses().get(TABLET_SERVER).iterator().next());

    while (true) {
      stats = getStats(c);
      if (2 != stats.tServerInfo.size()) {
        break;
      }
      UtilWaitThread.sleep(500);
    }
    assertEquals(1, stats.tServerInfo.size());
    assertEquals(1, stats.badTServers.size() + stats.deadTabletServers.size());
    while (true) {
      stats = getStats(c);
      if (0 != stats.deadTabletServers.size()) {
        break;
      }
      UtilWaitThread.sleep(500);
    }
    assertEquals(1, stats.tServerInfo.size());
    assertEquals(0, stats.badTServers.size());
    assertEquals(1, stats.deadTabletServers.size());
  }

  private MasterMonitorInfo getStats(Connector c) throws Exception {
    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    MasterClientService.Iface client = null;
    try {
      client = MasterClient.getConnectionWithRetry(c.getInstance(), false);
      log.info("Fetching master stats");
      return client.getMasterStats(Tracer.traceInfo(), creds.toThrift(c.getInstance()));
    } finally {
      if (client != null) {
        MasterClient.close(client);
      }
    }
  }


}
