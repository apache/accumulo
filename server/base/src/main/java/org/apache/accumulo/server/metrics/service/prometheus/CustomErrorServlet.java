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
import java.io.PrintWriter;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomErrorServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CustomErrorServlet.class);

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    response.setContentType("text/html");
    response.setCharacterEncoding("UTF-8");

    PrintWriter writer = response.getWriter();

    try {

      log.debug("Request to invalid metrics url: {}", request.getRequestURI());

      writer.println("<!DOCTYPE html>\n" + "<html lang=\"en\">\n" + "<head>\n"
          + "    <meta charset=\"UTF-8\">\n" + "    <title>Error</title>\n" + "</head>\n"
          + "<body>\n" + "Error - Page Not Found<p/>\n");
      writer.println("Invalid url: " + request.getRequestURI() + "\n");

      writer.println("</body>\n" + "</html>\n");

      response.setStatus(HttpServletResponse.SC_NOT_FOUND);

    } catch (Exception ex) {
      log.info("request to invalid url - failed with exception", ex);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
