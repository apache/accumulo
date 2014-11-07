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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.servlet.http.HttpServlet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.commons.lang.StringUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.SessionHandler;

public class EmbeddedWebServer {
  private static String EMPTY = "";

  Server server = null;
  SocketConnector sock;
  ContextHandlerCollection handler;
  Context root;
  boolean usingSsl;

  public EmbeddedWebServer() {
    this("0.0.0.0", 0);
  }

  public EmbeddedWebServer(String host, int port) {
    server = new Server();
    handler = new ContextHandlerCollection();
    root = new Context(handler, "/", new SessionHandler(), null, null, null);

    if (EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTORE))
        || EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTOREPASS))
        || EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTORE))
        || EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTOREPASS))) {
      sock = new SocketConnector();
      usingSsl = false;
    } else {
      SslSocketConnector sslSock = new SslSocketConnector();
      AccumuloConfiguration conf = Monitor.getSystemConfiguration();

      // Restrict the protocols on the server socket
      final String includeProtocols = conf.get(Property.MONITOR_SSL_INCLUDE_PROTOCOLS);
      if (null != includeProtocols && !includeProtocols.isEmpty()) {
        String[] protocols = StringUtils.split(includeProtocols, ',');
        sslSock = new TLSSocketConnector(protocols);
      }

      sslSock.setKeystore(conf.get(Property.MONITOR_SSL_KEYSTORE));
      sslSock.setKeyPassword(conf.get(Property.MONITOR_SSL_KEYSTOREPASS));
      sslSock.setTruststore(conf.get(Property.MONITOR_SSL_TRUSTSTORE));
      sslSock.setTrustPassword(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS));

      usingSsl = true;
      sock = sslSock;
    }
    sock.setHost(host);
    sock.setPort(port);
  }

  /**
   * Wrap the SocketConnector so the ServerSocket can be manipulated
   */
  protected static class TLSSocketConnector extends SslSocketConnector {

    private final String[] protocols;

    protected TLSSocketConnector(String[] protocols) {
      this.protocols = protocols;
    }

    @Override
    protected SSLServerSocketFactory createFactory() throws Exception {
      return new TLSServerSocketFactory(super.createFactory(), protocols);
    }
  }

  /**
   * Restrict the allowed protocols to TLS on the ServerSocket
   */
  protected static class TLSServerSocketFactory extends SSLServerSocketFactory {

    private final SSLServerSocketFactory delegate;
    private final String[] protocols;

    public TLSServerSocketFactory(SSLServerSocketFactory delegate, String[] protocols) {
      this.delegate = delegate;
      this.protocols = protocols;
    }

    @Override
    public ServerSocket createServerSocket() throws IOException {
      SSLServerSocket socket = (SSLServerSocket) delegate.createServerSocket();
      return overrideProtocol(socket);
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
      SSLServerSocket socket = (SSLServerSocket) delegate.createServerSocket(port);
      return overrideProtocol(socket);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog) throws IOException {
      SSLServerSocket socket = (SSLServerSocket) delegate.createServerSocket(port, backlog);
      return overrideProtocol(socket);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
      SSLServerSocket socket = (SSLServerSocket) delegate.createServerSocket(port, backlog);
      return overrideProtocol(socket);
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    protected ServerSocket overrideProtocol(SSLServerSocket socket) {
      socket.setEnabledProtocols(protocols);
      return socket;
    }

  }

  public void addServlet(Class<? extends HttpServlet> klass, String where) {
    root.addServlet(klass, where);
  }

  public int getPort() {
    return sock.getLocalPort();
  }

  public void start() {
    try {
      server.addConnector(sock);
      server.setHandler(handler);
      server.start();
    } catch (Exception e) {
      stop();
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isUsingSsl() {
    return usingSsl;
  }
}
