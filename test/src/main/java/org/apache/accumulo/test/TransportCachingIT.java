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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ThriftTransportKey;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that {@link ThriftTransportPool} actually adheres to the cachedConnection argument
 */
public class TransportCachingIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(TransportCachingIT.class);

  @Test
  public void testCachedTransport() throws InterruptedException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      List<String> tservers;

      while ((tservers = client.instanceOperations().getTabletServers()).isEmpty()) {
        // sleep until a tablet server is up
        Thread.sleep(50);
      }

      ClientContext context = (ClientContext) client;
      long rpcTimeout =
          ConfigurationTypeHelper.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT.getDefaultValue());

      List<ThriftTransportKey> servers = tservers.stream().map(serverStr -> {
        return new ThriftTransportKey(ThriftClientTypes.CLIENT, HostAndPort.fromString(serverStr),
            rpcTimeout, context);
      }).collect(Collectors.toList());

      // only want to use one server for all subsequent test
      ThriftTransportKey ttk = servers.get(0);

      ThriftTransportPool pool = context.getTransportPool();
      TTransport first = getAnyTransport(ttk, pool, true);

      assertNotNull(first);
      // Return it to unreserve it
      pool.returnTransport(first);

      TTransport second = getAnyTransport(ttk, pool, true);

      // We should get the same transport
      assertSame(first, second, "Expected the first and second to be the same instance");
      pool.returnTransport(second);

      // Ensure does not get cached connection just returned
      TTransport third = getAnyTransport(ttk, pool, false);
      assertNotSame(second, third, "Expected second and third transport to be different instances");

      TTransport fourth = getAnyTransport(ttk, pool, false);
      assertNotSame(third, fourth, "Expected third and fourth transport to be different instances");

      pool.returnTransport(third);
      pool.returnTransport(fourth);

      // The following three asserts ensure the per server queue is LIFO
      TTransport fifth = getAnyTransport(ttk, pool, true);
      assertSame(fourth, fifth, "Expected fourth and fifth transport to be the same instance");

      TTransport sixth = getAnyTransport(ttk, pool, true);
      assertSame(third, sixth, "Expected third and sixth transport to be the same instance");

      TTransport seventh = getAnyTransport(ttk, pool, true);
      assertSame(second, seventh, "Expected second and seventh transport to be the same instance");

      pool.returnTransport(fifth);
      pool.returnTransport(sixth);
      pool.returnTransport(seventh);
    }
  }

  private TTransport getAnyTransport(ThriftTransportKey ttk, ThriftTransportPool pool,
      boolean preferCached) {
    if (preferCached) {
      Pair<String,TTransport> cached = pool.getAnyCachedTransport(ttk.getType());
      if (cached != null) {
        return cached.getSecond();
      }
    }
    try {
      return pool.getTransport(ttk.getType(), ttk.getServer(), ttk.getTimeout(), getServerContext(),
          preferCached);
    } catch (TTransportException e) {
      log.warn("Failed to obtain transport to {}", ttk.getServer());
    }
    return null;
  }

}
