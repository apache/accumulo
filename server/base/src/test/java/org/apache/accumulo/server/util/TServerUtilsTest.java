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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ClientService.Iface;
import org.apache.accumulo.core.client.impl.thrift.ClientService.Processor;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.After;
import org.junit.Test;

public class TServerUtilsTest {

  protected static class TestInstance implements Instance {

    @Override
    public String getRootTabletLocation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMasterLocations() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getInstanceID() {
      return "1111";
    }

    @Override
    public String getInstanceName() {
      return "test";
    }

    @Override
    public String getZooKeepers() {
      return "";
    }

    @Override
    public int getZooKeepersSessionTimeOut() {
      return 30;
    }

    @Deprecated
    @Override
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException();
    }

  }

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    private ConfigurationCopy conf = null;

    public TestServerConfigurationFactory(Instance instance) {
      super(instance);
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

  private static final TestInstance instance = new TestInstance();
  private static final TestServerConfigurationFactory factory = new TestServerConfigurationFactory(instance);

  @After
  public void resetProperty() {
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, Property.TSERV_CLIENTPORT.getDefaultValue());
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_PORTSEARCH, Property.TSERV_PORTSEARCH.getDefaultValue());
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
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, Integer.toString(port));
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

  @Test(expected = UnknownHostException.class)
  public void testStartServerUsedPort() throws Exception {
    int port = getFreePort(1024);
    InetAddress addr = InetAddress.getByName("localhost");
    // Bind to the port
    ServerSocket s = new ServerSocket(port, 50, addr);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, Integer.toString(port));
    try {
      startServer();
    } finally {
      s.close();
    }
  }

  @Test
  public void testStartServerUsedPortWithSearch() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    // Bind to the port
    InetAddress addr = InetAddress.getByName("localhost");
    ServerSocket s = new ServerSocket(port[0], 50, addr);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, Integer.toString(port[0]));
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_PORTSEARCH, "true");
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertEquals(port[1], address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
      s.close();
    }
  }

  @Test
  public void testStartServerPortRange() throws Exception {
    TServer server = null;
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = Integer.toString(port[0]) + "-" + Integer.toString(port[1]);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, portRange);
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(port[0] == address.getAddress().getPort() || port[1] == address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
    }
  }

  @Test
  public void testStartServerPortRangeFirstPortUsed() throws Exception {
    TServer server = null;
    InetAddress addr = InetAddress.getByName("localhost");
    int[] port = findTwoFreeSequentialPorts(1024);
    String portRange = Integer.toString(port[0]) + "-" + Integer.toString(port[1]);
    // Bind to the port
    ServerSocket s = new ServerSocket(port[0], 50, addr);
    ((ConfigurationCopy) factory.getSystemConfiguration()).set(Property.TSERV_CLIENTPORT, portRange);
    try {
      ServerAddress address = startServer();
      assertNotNull(address);
      server = address.getServer();
      assertNotNull(server);
      assertTrue(port[1] == address.getAddress().getPort());
    } finally {
      if (null != server) {
        TServerUtils.stopTServer(server);
      }
      s.close();
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
    AccumuloServerContext ctx = new AccumuloServerContext(instance, factory);
    ClientServiceHandler clientHandler = new ClientServiceHandler(ctx, null, null);
    Iface rpcProxy = RpcWrapper.service(clientHandler);
    Processor<Iface> processor = new Processor<>(rpcProxy);
    // "localhost" explicitly to make sure we can always bind to that interface (avoids DNS misconfiguration)
    String hostname = "localhost";

    return TServerUtils.startServer(ctx, hostname, Property.TSERV_CLIENTPORT, processor, "TServerUtilsTest", "TServerUtilsTestThread",
        Property.TSERV_PORTSEARCH, Property.TSERV_MINTHREADS, Property.TSERV_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);

  }
}
