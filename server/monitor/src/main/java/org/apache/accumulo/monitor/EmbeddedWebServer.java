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

import org.apache.accumulo.core.conf.Property;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
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
    if (EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTORE))
        || EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTOREPASS))
        || EMPTY.equals(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTORE)) || EMPTY.equals(Monitor.getSystemConfiguration().get(
Property.MONITOR_SSL_TRUSTSTOREPASS))) {
      connector = new ServerConnector(server, new HttpConnectionFactory());
      usingSsl = false;
    } else {
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTORE));
      sslContextFactory.setKeyStorePassword(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTOREPASS));
      sslContextFactory.setTrustStorePath(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTORE));
      sslContextFactory.setTrustStorePassword(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTOREPASS));

      connector = new ServerConnector(server, sslContextFactory);
      usingSsl = true;
    }

    connector.setHost(host);
    connector.setPort(port);

    handler = new ServletContextHandler(server, "/", new SessionHandler(), null, null, null);
  }

  public void addServlet(Class<? extends HttpServlet> klass, String where) {
    handler.addServlet(klass, where);
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
