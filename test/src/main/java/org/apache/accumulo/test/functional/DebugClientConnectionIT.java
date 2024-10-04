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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.rpc.clients.TServerClient;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugClientConnectionIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(2);
  }

  private Set<ServerId> tservers = null;

  @BeforeEach
  public void getTServerAddresses() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      tservers = client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    }
    assertNotNull(tservers);
    assertEquals(2, tservers.size());
  }

  @Test
  public void testPreferredConnection() throws Exception {
    Iterator<ServerId> tsi = tservers.iterator();
    System.setProperty(TServerClient.DEBUG_HOST, tsi.next().toHostPortString());
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      assertNotNull(client.instanceOperations().getSiteConfiguration());
    }
    System.setProperty(TServerClient.DEBUG_HOST, tsi.next().toHostPortString());
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      assertNotNull(client.instanceOperations().getSiteConfiguration());
    }
    System.setProperty(TServerClient.DEBUG_HOST, "localhost:1");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      assertThrows(UncheckedIOException.class,
          () -> client.instanceOperations().getSiteConfiguration());
    }
  }
}
