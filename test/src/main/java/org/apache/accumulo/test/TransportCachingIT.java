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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ThriftTransportKey;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that {@link ThriftTransportPool} actually adheres to the cachedConnection argument
 */
public class TransportCachingIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(TransportCachingIT.class);
  private static int ATTEMPTS = 0;

  @Test
  public void testCachedTransport() throws InterruptedException {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      while (client.instanceOperations().getTabletServers().isEmpty()) {
        // sleep until a tablet server is up
        Thread.sleep(50);
      }
      ClientContext context = (ClientContext) client;
      long rpcTimeout =
          ConfigurationTypeHelper.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT.getDefaultValue());

      ZooCache zc = context.getZooCache();
      final String zkRoot = context.getZooKeeperRoot();

      // wait until Zookeeper is populated
      List<String> children = zc.getChildren(zkRoot + Constants.ZTSERVERS);
      while (children.isEmpty()) {
        Thread.sleep(100);
        children = zc.getChildren(zkRoot + Constants.ZTSERVERS);
      }

      ArrayList<ThriftTransportKey> servers = new ArrayList<>();
      while (servers.isEmpty()) {
        for (String tserver : children) {
          String path = zkRoot + Constants.ZTSERVERS + "/" + tserver;
          byte[] data = ZooUtil.getLockData(zc, path);
          if (data != null) {
            String strData = new String(data, UTF_8);
            if (!strData.equals("master"))
              servers.add(new ThriftTransportKey(
                  new ServerServices(strData).getAddress(Service.TSERV_CLIENT), rpcTimeout,
                  context));
          }
        }
        ATTEMPTS++;
        if (!servers.isEmpty())
          break;
        else {
          if (ATTEMPTS < 100) {
            log.warn("Making another attempt to add ThriftTransportKey servers");
            Thread.sleep(100);
          } else {
            log.error("Failed to add ThriftTransportKey servers - Failing TransportCachingIT test");
            org.junit.Assert
                .fail("Failed to add ThriftTransportKey servers - Failing TransportCachingIT test");
          }
        }
      }

      ThriftTransportPool pool = ThriftTransportPool.getInstance();
      TTransport first = null;
      while (first == null) {
        try {
          // Get a transport (cached or not)
          first = pool.getAnyTransport(servers, true).getSecond();
        } catch (TTransportException e) {
          log.warn("Failed to obtain transport to {}", servers);
        }
      }

      assertNotNull(first);
      // Return it to unreserve it
      pool.returnTransport(first);

      TTransport second = null;
      while (second == null) {
        try {
          // Get a cached transport (should be the first)
          second = pool.getAnyTransport(servers, true).getSecond();
        } catch (TTransportException e) {
          log.warn("Failed obtain 2nd transport to {}", servers);
        }
      }

      // We should get the same transport
      assertSame("Expected the first and second to be the same instance", first, second);
      // Return the 2nd
      pool.returnTransport(second);

      TTransport third = null;
      while (third == null) {
        try {
          // Get a non-cached transport
          third = pool.getAnyTransport(servers, false).getSecond();
        } catch (TTransportException e) {
          log.warn("Failed obtain 2nd transport to {}", servers);
        }
      }

      assertNotSame("Expected second and third transport to be different instances", second, third);
      pool.returnTransport(third);
    }
  }
}
