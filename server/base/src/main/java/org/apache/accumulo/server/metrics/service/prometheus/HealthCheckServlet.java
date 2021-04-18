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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(HealthCheckServlet.class);

  private static final DateTimeFormatter isoFmt =
      ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  private transient HealthCheck healthCheck = null;

  @Override
  public void init() {
    healthCheck = HealthCheck.getInstance();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    response.setContentType("text/plain");
    response.setCharacterEncoding("UTF-8");

    PrintWriter writer = response.getWriter();

    try {

      writer.println(String.format("Report Time: %s\n", isoFmt.format(Instant.now())));

      writer.println(getProcessStats());

      response.setStatus(HttpServletResponse.SC_OK);

    } catch (Exception ex) {
      writer.println("Error occurred creating health check report - check log");
      log.warn("health check report failed", ex);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private String getProcessStats() {
    StringBuilder sb = new StringBuilder();
    List<ProcessReport.Record> hc = healthCheck.getProcessReport();
    sb.append("Process Report:\n");
    sb.append("not implemented");
    return sb.toString();
  }

}
