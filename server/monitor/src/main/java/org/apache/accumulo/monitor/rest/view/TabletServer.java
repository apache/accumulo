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
package org.apache.accumulo.monitor.rest.view;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.CookieParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.Level;
import org.eclipse.jetty.server.Request;
import org.glassfish.jersey.server.mvc.Viewable;

@Path("/tservers")
public class TabletServer {

  @Context
  private Request request;

  @GET
  public Viewable get(@CookieParam("page.refresh.rate ") @DefaultValue("-1") String refreshValue) {
    int refresh = -1;
    try {
      refresh = Integer.parseInt(refreshValue);
    } catch (NumberFormatException e) {}

    List<DedupedLogEvent> logs = LogService.getInstance().getEvents();
    boolean logsHaveError = false;
    for (DedupedLogEvent dedupedLogEvent : logs) {
      if (dedupedLogEvent.getEvent().getLevel().isGreaterOrEqual(Level.ERROR)) {
        logsHaveError = true;
        break;
      }
    }

    int numProblems = Monitor.getProblemSummary().entrySet().size();

    String redir = request.getRequestURI();
    if (request.getQueryString() != null)
      redir += "?" + request.getQueryString();

    Map<String,Object> model = new HashMap<>();
    model.put("title", "Tablet Server Status");
    model.put("version", Constants.VERSION);
    model.put("refresh", refresh);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    model.put("current_date", new Date().toString().replace(" ", "&nbsp;"));
    model.put("num_logs", logs.size());
    model.put("logs_have_error", logsHaveError);
    model.put("num_problems", numProblems);
    model.put("is_ssl", false);
    model.put("redirect", redir);

    return new Viewable("tservers.ftl", model);
  }

  @Path("{server}")
  @GET
  public Viewable getServer(@PathParam("server") String server, @CookieParam("page.refresh.rate ") @DefaultValue("-1") String refreshValue) {
    int refresh = -1;
    try {
      refresh = Integer.parseInt(refreshValue);
    } catch (NumberFormatException e) {}

    List<DedupedLogEvent> logs = LogService.getInstance().getEvents();
    boolean logsHaveError = false;
    for (DedupedLogEvent dedupedLogEvent : logs) {
      if (dedupedLogEvent.getEvent().getLevel().isGreaterOrEqual(Level.ERROR)) {
        logsHaveError = true;
        break;
      }
    }

    int numProblems = Monitor.getProblemSummary().entrySet().size();

    String redir = request.getRequestURI();
    if (request.getQueryString() != null)
      redir += "?" + request.getQueryString();

    Map<String,Object> model = new HashMap<>();
    model.put("title", "Tablet Server Status");
    model.put("version", Constants.VERSION);
    model.put("refresh", refresh);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    model.put("current_date", new Date().toString().replace(" ", "&nbsp;"));
    model.put("num_logs", logs.size());
    model.put("logs_have_error", logsHaveError);
    model.put("num_problems", numProblems);
    model.put("is_ssl", false);
    model.put("server", server);
    model.put("redirect", redir);

    return new Viewable("server.ftl", model);
  }

}
