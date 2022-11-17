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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.clientImpl.thrift.ClientService.Iface;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Processor;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.thrift.server.TServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TServerUtilsTest {

  private ServerContext context;
  private final ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());

  @BeforeEach
  public void createMockServerContext() {
    context = createMock(ServerContext.class);
    expect(context.getZooReader()).andReturn(null).anyTimes();
    expect(context.getZooReaderWriter()).andReturn(null).anyTimes();
    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
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
    replay(context);
  }

  @AfterEach
  public void verifyMockServerContext() {
    verify(context);
  }

  @Test
  public void testStartServerZeroPort() throws Exception {
    TServer server = null;
    conf.set(Property.TSERV_CLIENTPORT, "0");
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
    conf.set(Property.TSERV_CLIENTPORT, Integer.toString(port));
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
  public void testStartServerUsedPort() throws Exception {
    int port = getFreePort(1024);
    InetAddress addr = InetAddress.getByName("localhost");
    // Bind to the port
    conf.set(Property.TSERV_CLIENTPORT, Integer.toString(port));
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
    conf.set(Property.TSERV_CLIENTPORT, Integer.toString(port[0]));
    conf.set(Property.TSERV_PORTSEARCH, "true");
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

  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "socket for testing")
  @Test
  public void testStartServerNonDefaultPorts() throws Exception {
    TServer server = null;

    // This test finds 5 free ports in more-or-less a contiguous way and then
    // uses those port numbers to Accumulo services in the below (ascending) sequence
    // 0. TServer default client port (this test binds to this port to force a port search)
    // 1. GC
    // 2. Manager
    // 3. Monitor
    // 4. One free port - this is the one that we expect the TServer to finally use
    int[] ports = findTwoFreeSequentialPorts(1024);
    int tserverDefaultPort = ports[0];
    conf.set(Property.TSERV_CLIENTPORT, Integer.toString(tserverDefaultPort));
    int gcPort = ports[1];
    conf.set(Property.GC_PORT, Integer.toString(gcPort));

    ports = findTwoFreeSequentialPorts(gcPort + 1);
    int managerPort = ports[0];
    conf.set(Property.MANAGER_CLIENTPORT, Integer.toString(managerPort));
    int monitorPort = ports[1];
    conf.set(Property.MONITOR_PORT, Integer.toString(monitorPort));

    ports = findTwoFreeSequentialPorts(monitorPort + 1);
    int tserverFinalPort = ports[0];

    conf.set(Property.TSERV_PORTSEARCH, "true");

    // Ensure that the TServer client port we set above is NOT in the reserved ports
    Map<Integer,Property> reservedPorts =
        TServerUtils.getReservedPorts(conf, Property.TSERV_CLIENTPORT);
    assertFalse(reservedPorts.containsKey(tserverDefaultPort));

    // Ensure that all the ports we assigned (GC, Manager, Monitor) are included in the reserved
    // ports as returned by TServerUtils
    assertTrue(reservedPorts.containsKey(gcPort));
    assertTrue(reservedPorts.containsKey(managerPort));
    assertTrue(reservedPorts.containsKey(monitorPort));

    InetAddress addr = InetAddress.getByName("localhost");
    try (ServerSocket s = new ServerSocket(tserverDefaultPort, 50, addr)) {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);

      // Finally ensure that the TServer is using the last port (i.e. port search worked)
      assertTrue(address.getAddress().getPort() == tserverFinalPort);
    } finally {
      if (server != null) {
        server.stop();
      }

    }
  }

  @Test
  public void testStartServerPortRange() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = port[0] + "-" + port[1];
    conf.set(Property.TSERV_CLIENTPORT, portRange);
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
    conf.set(Property.TSERV_CLIENTPORT, portRange);
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
    ClientServiceHandler clientHandler = new ClientServiceHandler(context, null);
    Iface rpcProxy = TraceUtil.wrapService(clientHandler);
    Processor<Iface> processor = new Processor<>(rpcProxy);
    // "localhost" explicitly to make sure we can always bind to that interface (avoids DNS
    // misconfiguration)
    String hostname = "localhost";

    return TServerUtils.startServer(context, hostname, Property.TSERV_CLIENTPORT, processor,
        "TServerUtilsTest", "TServerUtilsTestThread", Property.TSERV_PORTSEARCH,
        Property.TSERV_MINTHREADS, Property.TSERV_MINTHREADS_TIMEOUT, Property.TSERV_THREADCHECK,
        Property.GENERAL_MAX_MESSAGE_SIZE);

  }
}
