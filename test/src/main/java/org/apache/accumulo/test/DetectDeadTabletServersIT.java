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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
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
      try (Scanner scanner =
          c.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.forEach((k, v) -> {});
      }
      Wait.waitFor(
          () -> c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size() == 2);

      log.info("Killing a tablet server");
      getCluster().killProcess(TABLET_SERVER,
          getCluster().getProcesses().get(TABLET_SERVER).iterator().next());

      Wait.waitFor(
          () -> c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size() == 1);
    }
  }
}
