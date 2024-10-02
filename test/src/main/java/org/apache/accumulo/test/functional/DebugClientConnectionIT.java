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
import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.rpc.clients.TServerClient;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugClientConnectionIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(2);
  }

  private List<String> tservers = null;

  @BeforeEach
  public void getTServerAddresses() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      tservers = client.instanceOperations().getTabletServers();
    }
    assertNotNull(tservers);
    assertEquals(2, tservers.size());
  }

  @Test
  public void testPreferredConnection() throws Exception {
    System.setProperty(TServerClient.DEBUG_HOST, tservers.get(0));
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      assertNotNull(client.instanceOperations().getSiteConfiguration());
    }
    System.setProperty(TServerClient.DEBUG_HOST, tservers.get(1));
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
