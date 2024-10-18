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
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
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
  private final ServletContextHandler handler;
  private final boolean secure;

  private int actualHttpPort;
  private ServerConnector connector;

  public EmbeddedWebServer(final Monitor monitor, int httpPort) {
    server = new Server();
    final AccumuloConfiguration conf = monitor.getContext().getConfiguration();
    secure = requireForSecure.stream().map(conf::get).allMatch(s -> s != null && !s.isEmpty());

    addConnectors(monitor, conf, httpPort, secure);

    handler =
        new ServletContextHandler(ServletContextHandler.SESSIONS | ServletContextHandler.SECURITY);
    handler.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
    handler.setContextPath("/");
  }

  private void addConnectors(Monitor monitor, AccumuloConfiguration conf, int httpPort,
      boolean secure) {

    boolean configureHttp2 = conf.getBoolean(Property.MONITOR_SUPPORT_HTTP2);

    final HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendServerVersion(false);

    if (secure) {
      LOG.debug("Configuring Jetty with TLS");
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

      if (!configureHttp2) {
        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);
        SslConnectionFactory tls =
            new SslConnectionFactory(sslContextFactory, http11.getProtocol());
        connector = new ServerConnector(server, tls, http11);
      } else {
        LOG.debug("Enabling http2 support");
        httpConfig.addCustomizer(new SecureRequestCustomizer());
        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol(http11.getProtocol());
        HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(httpConfig);
        SslConnectionFactory tls = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());
        connector = new ServerConnector(server, tls, alpn, http2, http11);
      }
      connector.setHost(monitor.getHostname());
      connector.setPort(httpPort);
      server.addConnector(connector);
    } else {
      LOG.debug("Configuring Jetty without TLS");
      if (!configureHttp2) {
        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);
        connector = new ServerConnector(server, http11);
      } else {
        LOG.debug("Enabling http2 support");
        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);
        HTTP2CServerConnectionFactory http2 = new HTTP2CServerConnectionFactory(httpConfig);
        connector = new ServerConnector(server, http11, http2);
      }
      connector.setHost(monitor.getHostname());
      connector.setPort(httpPort);
      server.addConnector(connector);
    }
  }

  public void addServlet(ServletHolder restServlet, String where) {
    handler.addServlet(restServlet, where);
  }

  public int getHttpPort() {
    return this.actualHttpPort;
  }

  public boolean isSecure() {
    return secure;
  }

  public void start() {
    try {
      server.setHandler(handler);
      server.start();
      this.actualHttpPort = connector.getLocalPort();
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
