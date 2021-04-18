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
import java.util.Objects;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.service.MetricsRegistrationService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

@AutoService(MetricsRegistrationService.class)
public class PrometheusMetricsRegistration implements MetricsRegistrationService {

  private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsRegistration.class);

  private static Exporter exporter;

  public static Exporter getExporter() {
    return exporter;
  }

  @Override
  public void register(final ServerContext context, final String serviceName,
      final Map<String,String> properties, final CompositeMeterRegistry registry) {
    log.info("Loading metrics service name: {}, to {}, with props: {}", serviceName, registry,
        properties);
    log.warn("ZZZ - Init prometheus metrics");
    exporter = Exporter.getInstance();
  }

  private static class MetricsHttpServer implements AutoCloseable {

    private final Server jetty;

    public MetricsHttpServer(final int port) {

      jetty = new Server();

      ServerConnector connector = new ServerConnector(jetty);
      connector.setPort(port);
      jetty.setConnectors(new Connector[] {connector});

      ServletHandler servletHandler = new ServletHandler();
      jetty.setHandler(servletHandler);

      servletHandler.addServletWithMapping(HealthCheckServlet.class, "/status");
      servletHandler.addServletWithMapping(PrometheusExporterServlet.class, "/metrics");
    }

    @Override
    public void close() throws Exception {

    }
  }

  static class Exporter {
    private static Exporter _instance;

    private Exporter() {}

    public static synchronized Exporter getInstance() {
      if (Objects.isNull(_instance)) {
        _instance = new Exporter();
      }
      return _instance;
    }

    public String prometheusScrape() {
      return "return something";
    }
  }
}
