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

import javax.servlet.http.HttpServlet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class EmbeddedWebServer {
  private static String EMPTY = "";

  Server server = null;
  ServerConnector connector = null;
  ServletContextHandler handler;
  boolean usingSsl;

  public EmbeddedWebServer() {
    this("0.0.0.0", 0);
  }

  public EmbeddedWebServer(String host, int port) {
    server = new Server();
    final AccumuloConfiguration conf = Monitor.getContext().getConfiguration();
    if (EMPTY.equals(conf.get(Property.MONITOR_SSL_KEYSTORE)) || EMPTY.equals(conf.get(Property.MONITOR_SSL_KEYSTOREPASS))
        || EMPTY.equals(conf.get(Property.MONITOR_SSL_TRUSTSTORE)) || EMPTY.equals(conf.get(Property.MONITOR_SSL_TRUSTSTOREPASS))) {
      connector = new ServerConnector(server, new HttpConnectionFactory());
      usingSsl = false;
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

      connector = new ServerConnector(server, sslContextFactory);
      usingSsl = true;
    }

    connector.setHost(host);
    connector.setPort(port);

    handler = new ServletContextHandler(server, "/", new SessionHandler(), new ConstraintSecurityHandler(), null, null);
    disableTrace("/");
  }

  public void addServlet(Class<? extends HttpServlet> klass, String where) {
    handler.addServlet(klass, where);
  }

  private void disableTrace(String where) {
    Constraint constraint = new Constraint();
    constraint.setName("Disable TRACE");
    constraint.setAuthenticate(true); // require auth, but no roles defined, so it'll never match

    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setConstraint(constraint);
    mapping.setMethod("TRACE");
    mapping.setPathSpec(where);

    ConstraintSecurityHandler security = (ConstraintSecurityHandler) handler.getSecurityHandler();
    security.addConstraintMapping(mapping);
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
