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

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class PrometheusExporterServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private transient PrometheusMetricsRegistration.Exporter metrics;

  @Override
  public void init() {
    metrics = PrometheusMetricsRegistration.getExporter();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String scrape = metrics.prometheusScrape();

    response.setContentType("text/plain");
    response.setCharacterEncoding("UTF-8");

    if (acceptsGZipEncoding(request)) {
      // noop - TODO stub if compression is desired
    }

    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(scrape);
  }

  private boolean acceptsGZipEncoding(HttpServletRequest httpRequest) {
    String acceptEncoding = httpRequest.getHeader("Accept-Encoding");

    return acceptEncoding != null && acceptEncoding.contains("gzip");
  }
}
