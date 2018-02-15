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
package org.apache.accumulo.test.replication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 *
 */
public class MultiTserverReplicationIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(MultiTserverReplicationIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(2);
  }

  @Test
  public void tserverReplicationServicePortsAreAdvertised() throws Exception {
    // Wait for the cluster to be up
    Connector conn = getConnector();
    Instance inst = conn.getInstance();

    // Wait for a tserver to come up to fulfill this request
    conn.tableOperations().create("foo");
    try (Scanner s = conn.createScanner("foo", Authorizations.EMPTY)) {
      Assert.assertEquals(0, Iterables.size(s));

      ZooReader zreader = new ZooReader(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut());
      Set<String> tserverHost = new HashSet<>();
      tserverHost.addAll(zreader.getChildren(ZooUtil.getRoot(inst) + Constants.ZTSERVERS));

      Set<HostAndPort> replicationServices = new HashSet<>();

      for (String tserver : tserverHost) {
        try {
          byte[] portData = zreader.getData(ZooUtil.getRoot(inst) + ReplicationConstants.ZOO_TSERVERS + "/" + tserver, null);
          HostAndPort replAddress = HostAndPort.fromString(new String(portData, UTF_8));
          replicationServices.add(replAddress);
        } catch (Exception e) {
          log.error("Could not find port for {}", tserver, e);
          Assert.fail("Did not find replication port advertisement for " + tserver);
        }
      }

      // Each tserver should also have equial replicaiton services running internally
      Assert.assertEquals("Expected an equal number of replication servicers and tservers", tserverHost.size(), replicationServices.size());
    }
  }

  @Test
  public void masterReplicationServicePortsAreAdvertised() throws Exception {
    // Wait for the cluster to be up
    Connector conn = getConnector();
    Instance inst = conn.getInstance();

    // Wait for a tserver to come up to fulfill this request
    conn.tableOperations().create("foo");
    try (Scanner s = conn.createScanner("foo", Authorizations.EMPTY)) {
      Assert.assertEquals(0, Iterables.size(s));

      ZooReader zreader = new ZooReader(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut());

      // Should have one master instance
      Assert.assertEquals(1, inst.getMasterLocations().size());

      // Get the master thrift service addr
      String masterAddr = Iterables.getOnlyElement(inst.getMasterLocations());

      // Get the master replication coordinator addr
      String replCoordAddr = new String(zreader.getData(ZooUtil.getRoot(inst) + Constants.ZMASTER_REPLICATION_COORDINATOR_ADDR, null), UTF_8);

      // They shouldn't be the same
      Assert.assertNotEquals(masterAddr, replCoordAddr);

      // Neither should be zero as the port
      Assert.assertNotEquals(0, HostAndPort.fromString(masterAddr).getPort());
      Assert.assertNotEquals(0, HostAndPort.fromString(replCoordAddr).getPort());
    }
  }
}
