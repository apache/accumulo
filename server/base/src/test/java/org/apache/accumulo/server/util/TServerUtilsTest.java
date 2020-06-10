/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.clientImpl.thrift.ClientService.Iface;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Processor;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TServerUtilsTest {

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    private ConfigurationCopy conf = null;

    public TestServerConfigurationFactory(ServerContext context) {
      super(context, SiteConfiguration.auto());
      conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    }

    @Override
    public synchronized AccumuloConfiguration getSystemConfiguration() {
      return conf;
    }

  }

  private static class TServerWithoutES extends TServer {
    boolean stopCalled;

    TServerWithoutES(TServerSocket socket) {
      super(new TServer.Args(socket));
      stopCalled = false;
    }

    @Override
    public void serve() {}

    @Override
    public void stop() {
      stopCalled = true;
    }
  }

  private static class TServerWithES extends TServerWithoutES {
    final ExecutorService executorService_;

    TServerWithES(TServerSocket socket) {
      super(socket);
      executorService_ = createMock(ExecutorService.class);
      expect(executorService_.shutdownNow()).andReturn(null);
      replay(executorService_);
    }
  }

  @Test
  public void testStopTServer_ES() {
    TServerSocket socket = createNiceMock(TServerSocket.class);
    TServerWithES s = new TServerWithES(socket);
    TServerUtils.stopTServer(s);
    assertTrue(s.stopCalled);
    verify(s.executorService_);
  }

  @Test
  public void testStopTServer_NoES() {
    TServerSocket socket = createNiceMock(TServerSocket.class);
    TServerWithoutES s = new TServerWithoutES(socket);
    TServerUtils.stopTServer(s);
    assertTrue(s.stopCalled);
  }

  @Test
  public void testStopTServer_Null() {
    TServerUtils.stopTServer(null);
    // not dying is enough
  }

  private static AccumuloConfiguration config =
      new ConfigurationCopy(DefaultConfiguration.getInstance());

  private static ServerContext createMockContext() {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    expect(context.getZooReaderWriter()).andReturn(null);
    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
    expect(context.getZooKeepers()).andReturn("").anyTimes();
    expect(context.getInstanceName()).andReturn("instance").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(1).anyTimes();
    expect(context.getInstanceID()).andReturn("11111").anyTimes();
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    return context;
  }

  private static ServerContext createReplayMockInfo() {
    ServerContext context = createMockContext();
    replay(context);
    return context;
  }

  private static final TestServerConfigurationFactory factory =
      new TestServerConfigurationFactory(createReplayMockInfo());

  @After
  public void resetProperty() {
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        Property.TSERV_CLIENTPORT.getDefaultValue());
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_PORTSEARCH,
        Property.TSERV_PORTSEARCH.getDefaultValue());
  }

  @Test
  public void testStartServerZeroPort() throws Exception {
    TServer server = null;
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, "0");
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(address.getAddress().getPort() > 1024);
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
    }
  }

  @Test
  public void testStartServerFreePort() throws Exception {
    TServer server = null;
    int port = getFreePort(1024);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        Integer.toString(port));
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port, address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test(expected = UnknownHostException.class)
  public void testStartServerUsedPort() throws Exception {
    int port = getFreePort(1024);
    InetAddress addr = InetAddress.getByName("localhost");
    // Bind to the port
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        Integer.toString(port));
    try (ServerSocket s = new ServerSocket(port, 50, addr)) {
      assertNotNull(s);
      startServer();
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerUsedPortWithSearch() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    // Bind to the port
    InetAddress addr = InetAddress.getByName("localhost");
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        Integer.toString(port[0]));
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_PORTSEARCH, "true");
    try (ServerSocket s = new ServerSocket(port[0], 50, addr)) {
      assertNotNull(s);
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port[1], address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }

    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerNonDefaultPorts() throws Exception {
    TServer server = null;

    // This test finds 6 free ports in more-or-less a contiguous way and then
    // uses those port numbers to Accumulo services in the below (ascending) sequence
    // 0. TServer default client port (this test binds to this port to force a port search)
    // 1. GC
    // 2. Master
    // 3. Monitor
    // 4. Master Replication Coordinator
    // 5. One free port - this is the one that we expect the TServer to finally use
    int[] ports = findTwoFreeSequentialPorts(1024);
    int tserverDefaultPort = ports[0];
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        Integer.toString(tserverDefaultPort));
    int gcPort = ports[1];
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.GC_PORT,
        Integer.toString(gcPort));

    ports = findTwoFreeSequentialPorts(gcPort + 1);
    int masterPort = ports[0];
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.MASTER_CLIENTPORT,
        Integer.toString(masterPort));
    int monitorPort = ports[1];
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.MONITOR_PORT,
        Integer.toString(monitorPort));

    ports = findTwoFreeSequentialPorts(monitorPort + 1);
    int masterReplCoordPort = ports[0];
    ((ConfigurationCopy) factory.getSystemConfiguration())
        .set(Property.MASTER_REPLICATION_COORDINATOR_PORT, Integer.toString(masterReplCoordPort));
    int tserverFinalPort = ports[1];

    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_PORTSEARCH, "true");

    // Ensure that the TServer client port we set above is NOT in the reserved ports
    Map<Integer,Property> reservedPorts =
        TServerUtils.getReservedPorts(factory.getSystemConfiguration());
    assertTrue(!reservedPorts.containsKey(tserverDefaultPort));

    // Ensure that all the ports we assigned (GC, Master, Monitor) are included in the reserved
    // ports as returned by TServerUtils
    assertTrue(reservedPorts.containsKey(gcPort));
    assertTrue(reservedPorts.containsKey(masterPort));
    assertTrue(reservedPorts.containsKey(monitorPort));
    assertTrue(reservedPorts.containsKey(masterReplCoordPort));

    InetAddress addr = InetAddress.getByName("localhost");
    try (ServerSocket s = new ServerSocket(tserverDefaultPort, 50, addr)) {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);

      // Finally ensure that the TServer is using the last port (i.e. port search worked)
      assertTrue(address.getAddress().getPort() == tserverFinalPort);
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }

    }
  }

  @Test
  public void testStartServerPortRange() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = Integer.toString(port[0]) + "-" + Integer.toString(port[1]);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        portRange);
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(
          port[0] == address.getAddress().getPort() || port[1] == address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerPortRangeFirstPortUsed() throws Exception {
    TServer server = null;
    InetAddress addr = InetAddress.getByName("localhost");
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = Integer.toString(port[0]) + "-" + Integer.toString(port[1]);
    // Bind to the port
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT,
        portRange);
    try (ServerSocket s = new ServerSocket(port[0], 50, addr)) {
      assertNotNull(s);
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port[1], address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
    }
  }

  private int[] findTwoFreeSequentialPorts(int startingAddress) throws UnknownHostException {
    boolean sequential = false;
    int low = startingAddress;
    int high = 0;
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
    throw new RuntimeException("Unable to find open port");
  }

  private ServerAddress startServer() throws Exception {
    ServerContext ctx = createMock(ServerContext.class);
    expect(ctx.getZooReaderWriter()).andReturn(null).anyTimes();
    expect(ctx.getInstanceID()).andReturn("instance").anyTimes();
    expect(ctx.getConfiguration()).andReturn(factory.getSystemConfiguration()).anyTimes();
    expect(ctx.getThriftServerType()).andReturn(ThriftServerType.THREADPOOL);
    expect(ctx.getServerSslParams()).andReturn(null).anyTimes();
    expect(ctx.getSaslParams()).andReturn(null).anyTimes();
    expect(ctx.getClientTimeoutInMillis()).andReturn((long) 1000).anyTimes();
    replay(ctx);
    ClientServiceHandler clientHandler = new ClientServiceHandler(ctx, null, null);
    Iface rpcProxy = TraceUtil.wrapService(clientHandler);
    Processor<Iface> processor = new Processor<>(rpcProxy);
    // "localhost" explicitly to make sure we can always bind to that interface (avoids DNS
    // misconfiguration)
    String hostname = "localhost";

    return TServerUtils.startServer(Metrics.initSystem(getClass().getSimpleName()), ctx, hostname,
        Property.TSERV_CLIENTPORT, processor, "TServerUtilsTest", "TServerUtilsTestThread",
        Property.TSERV_PORTSEARCH, Property.TSERV_MINTHREADS, Property.TSERV_THREADCHECK,
        Property.GENERAL_MAX_MESSAGE_SIZE);

  }
}
