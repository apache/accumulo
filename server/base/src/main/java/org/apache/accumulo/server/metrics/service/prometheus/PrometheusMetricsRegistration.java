/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.metrics.service.prometheus;

import java.util.Map;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.service.MetricsRegistrationService;
import org.apache.accumulo.server.metrics.service.MicrometerMetricsFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

@AutoService(MetricsRegistrationService.class)
public class PrometheusMetricsRegistration implements MetricsRegistrationService {

  private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsRegistration.class);

  private static Exporter exporter;
  private MetricsHttpServer server;

  public static Exporter getExporter() {
    return exporter;
  }

  @Override
  public void register(final ServerContext context, final String serviceName,
      final Map<String,String> properties, final CompositeMeterRegistry registry) {

    log.info("Loading metrics service name: {}, to {}, with props: {}", serviceName, registry,
        properties);
    log.warn("ZZZ - Init prometheus metrics");
    log.warn("ZZZ - service name: {}", serviceName);

    String appName = properties.get(MicrometerMetricsFactory.CALLING_SERVICE_NAME);
    log.warn("ZZZ - app name: {}", appName);
    int port = Integer.parseInt(properties.getOrDefault(appName + ".PortNumber", "0"));

    log.warn("ZZZ: pn from props: {}", port);
    log.warn("P:{}", properties);

    try {
      server = new MetricsHttpServer(port);
      exporter = new Exporter(registry);
    } catch (Exception ex) {
      log.warn("Failed to start prometheus http exporter - prometheus metrics will be unavailable",
          ex);
    }
  }

  public void stop() throws Exception {
    server.close();
  }

  private static class MetricsHttpServer implements AutoCloseable {

    private final Server jetty;
    private final int connectedPort;

    public MetricsHttpServer(final int port) throws Exception {

      jetty = new Server();

      ServerConnector connector = new ServerConnector(jetty);
      connector.setPort(port);
      jetty.setConnectors(new Connector[] {connector});

      ServletHandler servletHandler = new ServletHandler();
      jetty.setHandler(servletHandler);

      servletHandler.addServletWithMapping(HealthCheckServlet.class, "/status");
      servletHandler.addServletWithMapping(PrometheusExporterServlet.class, "/metrics");

      servletHandler.addServletWithMapping(CustomErrorServlet.class, "/*");

      jetty.setStopAtShutdown(true);
      jetty.setStopTimeout(5_000);

      jetty.start();

      connectedPort = ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();

      log.info("ZZZ: Metrics HTTP server port: {}", connectedPort);
    }

    public int getConnectedPort() {
      return connectedPort;
    }

    @Override
    public void close() throws Exception {
      jetty.stop();
    }
  }

  static class Exporter {

    private final PrometheusMeterRegistry prometheusRegistry;

    public Exporter(final CompositeMeterRegistry registry) {
      prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      registry.add(prometheusRegistry);
    }

    public String prometheusScrape() {
      return prometheusRegistry.scrape();
    }
  }
}
