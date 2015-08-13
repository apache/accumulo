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
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.mvc.mustache.MustacheMvcFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyMonitorApplication extends MonitorApplication {
  private static final Logger log = LoggerFactory.getLogger(JettyMonitorApplication.class);

  public JettyMonitorApplication() {}

  public void run() {
    Instance instance = HdfsZooInstance.getInstance();
    ServerConfigurationFactory config = new ServerConfigurationFactory(instance);
    AccumuloServerContext serverContext = new AccumuloServerContext(config);

    // Set the objects on the old monitor and start daemons to regularly poll the data
    startDataDaemons(config, instance, serverContext);

    final ResourceConfig rc = new ResourceConfig().register(MustacheMvcFeature.class).property(MustacheMvcFeature.TEMPLATE_BASE_PATH, "/templates")
        .packages("org.apache.accumulo.monitor.rest.api", "org.apache.accumulo.monitor.rest.resources", "org.apache.accumulo.monitor.rest.view")
        .property(ServerProperties.TRACING, "ALL").register(new LoggingFilter(java.util.logging.Logger.getLogger("JettyMonitorApplication"), true))
        .register(JacksonFeature.class).registerClasses(AccumuloExceptionMapper.class);

    final URI serverUri = getServerUri();

    Server server = JettyHttpContainerFactory.createServer(serverUri, rc);

    int port = serverUri.getPort();
    if (0 == port) {
      port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    String hostname = serverUri.getHost();

    log.info("Server bound to " + hostname + ":" + port);

    try {
      advertiseHttpAddress(serverContext.getConnector().getInstance(), hostname, port);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Failed to connect to Accumulo", e);
    }

    try {
      System.in.read();
    } catch (IOException e) {}

  }

  public static final void main(String[] args) throws Exception {
    new JettyMonitorApplication().run();
  }
}
