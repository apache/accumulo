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
package org.apache.accumulo.monitor;

import java.util.EnumSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedWebServer {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedWebServer.class);

  private static final EnumSet<Property> requireForSecure =
      EnumSet.of(Property.MONITOR_SSL_KEYSTORE, Property.MONITOR_SSL_KEYSTOREPASS,
          Property.MONITOR_SSL_TRUSTSTORE, Property.MONITOR_SSL_TRUSTSTOREPASS);

  private final Server server;
  private final ServerConnector connector;
  private final ServletContextHandler handler;
  private final boolean secure;

  public EmbeddedWebServer(Monitor monitor, int port) {
    server = new Server();
    final AccumuloConfiguration conf = monitor.getContext().getConfiguration();
    secure = requireForSecure.stream().map(conf::get).allMatch(s -> s != null && !s.isEmpty());

    connector = new ServerConnector(server, getConnectionFactories(conf, secure));
    connector.setHost(monitor.getHostname());
    connector.setPort(port);

    handler =
        new ServletContextHandler(ServletContextHandler.SESSIONS | ServletContextHandler.SECURITY);
    handler.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
    handler.setContextPath("/");
  }

  private static AbstractConnectionFactory[] getConnectionFactories(AccumuloConfiguration conf,
      boolean secure) {
    HttpConnectionFactory httpFactory = new HttpConnectionFactory();
    if (secure) {
      LOG.debug("Configuring Jetty to use TLS");
      final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
      // If the key password is the same as the keystore password, we don't
      // have to explicitly set it. Thus, if the user doesn't provide a key
      // password, don't set anything.
      final String keyPass = conf.get(Property.MONITOR_SSL_KEYPASS);
      if (!Property.MONITOR_SSL_KEYPASS.getDefaultValue().equals(keyPass)) {
        sslContextFactory.setKeyManagerPassword(keyPass);
      }
      sslContextFactory.setKeyStorePath(conf.get(Property.MONITOR_SSL_KEYSTORE));
      sslContextFactory.setKeyStorePassword(conf.get(Property.MONITOR_SSL_KEYSTOREPASS));
      sslContextFactory.setKeyStoreType(conf.get(Property.MONITOR_SSL_KEYSTORETYPE));
      sslContextFactory.setTrustStorePath(conf.get(Property.MONITOR_SSL_TRUSTSTORE));
      sslContextFactory.setTrustStorePassword(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS));
      sslContextFactory.setTrustStoreType(conf.get(Property.MONITOR_SSL_TRUSTSTORETYPE));

      final String includedCiphers = conf.get(Property.MONITOR_SSL_INCLUDE_CIPHERS);
      if (!Property.MONITOR_SSL_INCLUDE_CIPHERS.getDefaultValue().equals(includedCiphers)) {
        sslContextFactory.setIncludeCipherSuites(includedCiphers.split(","));
      }

      final String excludedCiphers = conf.get(Property.MONITOR_SSL_EXCLUDE_CIPHERS);
      if (!Property.MONITOR_SSL_EXCLUDE_CIPHERS.getDefaultValue().equals(excludedCiphers)) {
        sslContextFactory.setExcludeCipherSuites(excludedCiphers.split(","));
      }

      final String includeProtocols = conf.get(Property.MONITOR_SSL_INCLUDE_PROTOCOLS);
      if (includeProtocols != null && !includeProtocols.isEmpty()) {
        sslContextFactory.setIncludeProtocols(includeProtocols.split(","));
      }

      SslConnectionFactory sslFactory =
          new SslConnectionFactory(sslContextFactory, httpFactory.getProtocol());
      return new AbstractConnectionFactory[] {sslFactory, httpFactory};
    } else {
      LOG.debug("Not configuring Jetty to use TLS");
      return new AbstractConnectionFactory[] {httpFactory};
    }
  }

  public void addServlet(ServletHolder restServlet, String where) {
    handler.addServlet(restServlet, where);
  }

  public int getPort() {
    return connector.getLocalPort();
  }

  public boolean isSecure() {
    return secure;
  }

  public void start() {
    try {
      server.addConnector(connector);
      server.setHandler(handler);
      server.start();
    } catch (Exception e) {
      stop();
      throw new RuntimeException(e);
    }
  }

  private void stop() {
    try {
      server.stop();
      server.join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isRunning() {
    return server.isRunning();
  }

}
