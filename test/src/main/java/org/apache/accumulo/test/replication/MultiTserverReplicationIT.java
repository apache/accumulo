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
package org.apache.accumulo.test.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

@Disabled("Replication ITs are not stable and not currently maintained")
@Deprecated
public class MultiTserverReplicationIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(MultiTserverReplicationIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // set the name to kick off the replication services
    cfg.setProperty(Property.REPLICATION_NAME.getKey(), "test");
    cfg.setNumTservers(2);
  }

  @Test
  public void tserverReplicationServicePortsAreAdvertised() throws Exception {
    // Wait for the cluster to be up
    AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
    ClientContext context = (ClientContext) client;

    // Wait for a tserver to come up to fulfill this request
    client.tableOperations().create("foo");
    try (Scanner s = client.createScanner("foo", Authorizations.EMPTY)) {
      assertEquals(0, Iterables.size(s));

      ZooReader zreader = context.getZooReader();
      Set<String> tserverHost = new HashSet<>();
      tserverHost.addAll(zreader.getChildren(
          ZooUtil.getRoot(client.instanceOperations().getInstanceId()) + Constants.ZTSERVERS));

      Set<HostAndPort> replicationServices = new HashSet<>();

      for (String tserver : tserverHost) {
        try {
          byte[] portData =
              zreader.getData(ZooUtil.getRoot(client.instanceOperations().getInstanceId())
                  + ReplicationConstants.ZOO_TSERVERS + "/" + tserver);
          HostAndPort replAddress = HostAndPort.fromString(new String(portData, UTF_8));
          replicationServices.add(replAddress);
        } catch (Exception e) {
          log.error("Could not find port for {}", tserver, e);
          fail("Did not find replication port advertisement for " + tserver);
        }
      }

      // Each tserver should also have equal replication services running internally
      assertEquals(tserverHost.size(), replicationServices.size(),
          "Expected an equal number of replication servicers and tservers");
    }
  }

  @Test
  public void managerReplicationServicePortsAreAdvertised() throws Exception {
    // Wait for the cluster to be up
    AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
    ClientContext context = (ClientContext) client;

    // Wait for a tserver to come up to fulfill this request
    client.tableOperations().create("foo");
    try (Scanner s = client.createScanner("foo", Authorizations.EMPTY)) {
      assertEquals(0, Iterables.size(s));

      ZooReader zreader = context.getZooReader();

      // Should have one manager instance
      assertEquals(1, context.getManagerLocations().size());

      // Get the manager thrift service addr
      String managerAddr = getOnlyElement(context.getManagerLocations());

      // Get the manager replication coordinator addr
      String replCoordAddr =
          new String(zreader.getData(ZooUtil.getRoot(client.instanceOperations().getInstanceId())
              + Constants.ZMANAGER_REPLICATION_COORDINATOR_ADDR), UTF_8);

      // They shouldn't be the same
      assertNotEquals(managerAddr, replCoordAddr);

      // Neither should be zero as the port
      assertNotEquals(0, HostAndPort.fromString(managerAddr).getPort());
      assertNotEquals(0, HostAndPort.fromString(replCoordAddr).getPort());
    }
  }
}
