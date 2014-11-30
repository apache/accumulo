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
package org.apache.accumulo.monitor.rest;

import java.io.IOException;
import java.net.URI;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 *
 */
public class GrizzlyMonitorApplication extends MonitorApplication {
  private static final Logger log = Logger.getLogger(GrizzlyMonitorApplication.class);

  @Override
  public void run() {
    final ResourceConfig rc = new ResourceConfig().packages("org.apache.accumulo.monitor.rest.api", "org.apache.accumulo.monitor.rest.resources");
    // .property(ServerProperties.TRACING, "ALL").register(new LoggingFilter(java.util.logging.Logger.getLogger("GrizzlyMonitorApplication"), true));

    final URI serverUri = getServerUri();

    HttpServer server = GrizzlyHttpServerFactory.createHttpServer(serverUri, rc);

    int port = serverUri.getPort();
    if (0 == port) {
      port = server.getListener("grizzly").getPort();
    }

    String hostname = serverUri.getHost();

    log.info("Server bound to " + hostname + ":" + port);

    Instance instance = HdfsZooInstance.getInstance();
    ServerConfigurationFactory config = new ServerConfigurationFactory(instance);
    AccumuloServerContext context = new AccumuloServerContext(config);

    Monitor.setConfig(config);
    Monitor.setInstance(instance);
    Monitor.setContext(context);

    try {
      advertiseHttpAddress(context.getConnector().getInstance(), hostname, port);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Failed to connect to Accumulo", e);
    }

    // need to regularly fetch data so plot data is updated
    new Daemon(new LoggingRunnable(log, new Runnable() {

      @Override
      public void run() {
        while (true) {
          try {
            Monitor.fetchData();
          } catch (Exception e) {
            log.warn(e.getMessage(), e);
          }

          UtilWaitThread.sleep(333);
        }

      }
    }), "Data fetcher").start();

    try {
      System.in.read();
    } catch (IOException e) {}
  }

  public static final void main(String[] args) throws Exception {
    new GrizzlyMonitorApplication().run();
  }

}
