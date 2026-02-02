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
package org.apache.accumulo.server.rpc;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.apache.accumulo.core.clientImpl.thrift.ClientService.Iface;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Processor;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.thrift.server.TServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TServerUtilsTest {

  private ServerContext context;
  private ZooSession zk;
  private MetricsInfo metricsInfo;
  private ConfigurationCopy conf;

  @BeforeEach
  public void createMockServerContext() {
    conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    context = createMock(ServerContext.class);
    zk = createMock(ZooSession.class);
    expect(context.getZooSession()).andReturn(zk).anyTimes();
    expect(zk.asReader()).andReturn(null).anyTimes();
    expect(zk.asReaderWriter()).andReturn(null).anyTimes();
    expect(context.getZooKeepers()).andReturn("").anyTimes();
    expect(context.getInstanceName()).andReturn("instance").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(1).anyTimes();
    expect(context.getInstanceID()).andReturn(InstanceId.of("11111")).anyTimes();
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getThriftServerType()).andReturn(ThriftServerType.THREADPOOL).anyTimes();
    expect(context.getServerSslParams()).andReturn(null).anyTimes();
    expect(context.getSaslParams()).andReturn(null).anyTimes();
    expect(context.getClientTimeoutInMillis()).andReturn((long) 1000).anyTimes();
    expect(context.getSecurityOperation()).andReturn(null).anyTimes();
    metricsInfo = createMock(MetricsInfo.class);
    metricsInfo.addMetricsProducers(anyObject(MetricsProducer.class));
    expectLastCall().anyTimes();
    expect(context.getMetricsInfo()).andReturn(metricsInfo).anyTimes();
    replay(context, metricsInfo);
  }

  @AfterEach
  public void verifyMockServerContext() {
    verify(context, metricsInfo);
  }

  @Test
  public void testStartServerZeroPort() throws Exception {
    TServer server = null;
    conf.set(Property.RPC_BIND_PORT, "0");
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(address.getAddress().getPort() > 1024);
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  public void testStartServerFreePort() throws Exception {
    TServer server = null;
    int port = getFreePort(1024);
    conf.set(Property.RPC_BIND_PORT, Integer.toString(port));
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port, address.getAddress().getPort());
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerUsedPortFails() throws Exception {
    InetAddress addr = InetAddress.getByName("localhost");
    int port = getFreePort(1024);
    // Bind to the port
    conf.set(Property.RPC_BIND_PORT, Integer.toString(port));
    try (ServerSocket s = new ServerSocket(port, 50, addr)) {
      assertNotNull(s);
      assertThrows(UnknownHostException.class, this::startServer);
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerUsedPortWithSearch() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    // Bind to the port
    InetAddress addr = InetAddress.getByName("localhost");
    conf.set(Property.RPC_BIND_PORT, port[0] + "-" + port[1]);
    try (ServerSocket s = new ServerSocket(port[0], 50, addr)) {
      assertNotNull(s);
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port[1], address.getAddress().getPort());
    } finally {
      if (server != null) {
        server.stop();
      }

    }
  }

  /**
   * Make sure an exception is thrown if the given port range has no available ports in it
   */
  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerPortRangeAllUsedFails() throws Exception {
    InetAddress addr = InetAddress.getByName("localhost");
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = port[0] + "-" + port[1];
    conf.set(Property.RPC_BIND_PORT, portRange);
    try (ServerSocket s1 = new ServerSocket(port[0], 50, addr);
        ServerSocket s2 = new ServerSocket(port[1], 50, addr)) {
      assertNotNull(s1);
      assertNotNull(s2);
      assertThrows(UnknownHostException.class, this::startServer);
    }
  }

  @Test
  public void testStartServerPortRange() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = port[0] + "-" + port[1];
    conf.set(Property.RPC_BIND_PORT, portRange);
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(
          port[0] == address.getAddress().getPort() || port[1] == address.getAddress().getPort());
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerPortRangeFirstPortUsed() throws Exception {
    TServer server = null;
    InetAddress addr = InetAddress.getByName("localhost");
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = port[0] + "-" + port[1];
    // Bind to the port
    conf.set(Property.RPC_BIND_PORT, portRange);
    try (ServerSocket s = new ServerSocket(port[0], 50, addr)) {
      assertNotNull(s);
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port[1], address.getAddress().getPort());
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private int[] findTwoFreeSequentialPorts(int startingAddress) throws UnknownHostException {
    boolean sequential;
    int low = startingAddress;
    int high;
    do {
      low = getFreePort(low);
      high = getFreePort(low + 1);
      sequential = ((high - low) == 1);
    } while (!sequential);
    return new int[] {low, high};
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  private int getFreePort(int startingAddress) throws UnknownHostException {
    final InetAddress addr = InetAddress.getByName("localhost");
    for (int i = startingAddress; i < 65535; i++) {
      try {
        ServerSocket s = new ServerSocket(i, 50, addr);
        int port = s.getLocalPort();
        s.close();
        return port;
      } catch (IOException e) {
        // keep trying
      }
    }
    throw new IllegalStateException("Unable to find open port");
  }

  private ServerAddress startServer() throws Exception {
    ClientServiceHandler clientHandler = new ClientServiceHandler(context);
    Iface rpcProxy = TraceUtil.wrapService(clientHandler);
    Processor<Iface> processor = new Processor<>(rpcProxy);
    // "localhost" explicitly to make sure we can always bind to that interface (avoids DNS
    // misconfiguration)
    String hostname = "localhost";

    ServerAddress sa = TServerUtils.createThriftServer(context, hostname, processor,
        "TServerUtilsTest", Property.TSERV_MINTHREADS, Property.TSERV_MINTHREADS_TIMEOUT,
        Property.TSERV_THREADCHECK);
    sa.startThriftServer("TServerUtilsTestThread");
    return sa;
  }
}
