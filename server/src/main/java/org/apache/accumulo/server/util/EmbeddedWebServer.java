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

import javax.servlet.http.HttpServlet;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.monitor.Monitor;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.SessionHandler;

public class EmbeddedWebServer {
  
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
    
    if (Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTORE) == ""
        || Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTOREPASS) == ""
        || Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTORE) == ""
        || Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTOREPASS) == "") {
      sock = new SocketConnector();
      usingSsl = false;
    } else {
      sock = new SslSocketConnector();
      ((SslSocketConnector) sock).setKeystore(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTORE));
      ((SslSocketConnector) sock).setKeyPassword(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_KEYSTOREPASS));
      ((SslSocketConnector) sock).setTruststore(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTORE));
      ((SslSocketConnector) sock).setTrustPassword(Monitor.getSystemConfiguration().get(Property.MONITOR_SSL_TRUSTSTOREPASS));
      usingSsl = true;
    }
    sock.setHost(host);
    sock.setPort(port);
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
