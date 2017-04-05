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
package org.apache.accumulo.monitor;

import java.util.EnumSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class EmbeddedWebServer {
  private final Server server;
  private final ServerConnector connector;
  private final ServletContextHandler handler;

  public EmbeddedWebServer(String host, int port) {
    server = new Server();
    final AccumuloConfiguration conf = Monitor.getContext().getConfiguration();
    connector = new ServerConnector(server, getConnectionFactory(conf));
    connector.setHost(host);
    connector.setPort(port);

    handler = new ServletContextHandler(ServletContextHandler.SESSIONS | ServletContextHandler.SECURITY);
    handler.getSessionHandler().getSessionManager().getSessionCookieConfig().setHttpOnly(true);
    handler.setContextPath("/");
  }

  private static AbstractConnectionFactory getConnectionFactory(AccumuloConfiguration conf) {
    EnumSet<Property> requireForSecure = EnumSet.of(Property.MONITOR_SSL_KEYSTORE, Property.MONITOR_SSL_KEYSTOREPASS, Property.MONITOR_SSL_TRUSTSTORE,
        Property.MONITOR_SSL_TRUSTSTOREPASS);
    if (requireForSecure.stream().map(p -> conf.get(p)).anyMatch(s -> s == null || s.isEmpty())) {
      return new HttpConnectionFactory();
    } else {
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(conf.get(Property.MONITOR_SSL_KEYSTORE));
      sslContextFactory.setKeyStorePassword(conf.get(Property.MONITOR_SSL_KEYSTOREPASS));
      sslContextFactory.setKeyStoreType(conf.get(Property.MONITOR_SSL_KEYSTORETYPE));
      sslContextFactory.setTrustStorePath(conf.get(Property.MONITOR_SSL_TRUSTSTORE));
      sslContextFactory.setTrustStorePassword(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS));
      sslContextFactory.setTrustStoreType(conf.get(Property.MONITOR_SSL_TRUSTSTORETYPE));

      final String includedCiphers = conf.get(Property.MONITOR_SSL_INCLUDE_CIPHERS);
      if (!Property.MONITOR_SSL_INCLUDE_CIPHERS.getDefaultValue().equals(includedCiphers)) {
        sslContextFactory.setIncludeCipherSuites(StringUtils.split(includedCiphers, ','));
      }

      final String excludedCiphers = conf.get(Property.MONITOR_SSL_EXCLUDE_CIPHERS);
      if (!Property.MONITOR_SSL_EXCLUDE_CIPHERS.getDefaultValue().equals(excludedCiphers)) {
        sslContextFactory.setExcludeCipherSuites(StringUtils.split(excludedCiphers, ','));
      }

      final String includeProtocols = conf.get(Property.MONITOR_SSL_INCLUDE_PROTOCOLS);
      if (null != includeProtocols && !includeProtocols.isEmpty()) {
        sslContextFactory.setIncludeProtocols(StringUtils.split(includeProtocols, ','));
      }

      return new SslConnectionFactory(sslContextFactory, new HttpConnectionFactory().getProtocol());
    }
  }

  public void addServlet(ServletHolder restServlet, String where) {
    handler.addServlet(restServlet, where);
  }

  public int getPort() {
    return connector.getLocalPort();
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
