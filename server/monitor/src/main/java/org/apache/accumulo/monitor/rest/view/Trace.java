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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.Level;
import org.eclipse.jetty.server.Request;
import org.glassfish.jersey.server.mvc.Viewable;

@Path("/trace")
public class Trace {

  @Context
  private Request request;

  @Path("/summary")
  @GET
  public Viewable getSummary(@QueryParam("minutes") @DefaultValue("10") String minutes,
      @CookieParam("page.refresh.rate ") @DefaultValue("-1") String refreshValue) {
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
    model.put("title", "Traces for the last&nbsp;" + minutes + "&nbsp;minutes");
    model.put("version", Constants.VERSION);
    model.put("refresh", refresh);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    model.put("current_date", new Date().toString().replace(" ", "&nbsp;"));
    model.put("num_logs", logs.size());
    model.put("logs_have_error", logsHaveError);
    model.put("num_problems", numProblems);
    model.put("is_ssl", false);
    model.put("minutes", minutes);
    model.put("redirect", redir);

    return new Viewable("summary.ftl", model);
  }

  @Path("/listType")
  @GET
  public Viewable getType(@QueryParam("type") String type, @QueryParam("minutes") @DefaultValue("10") String minutes,
      @CookieParam("page.refresh.rate ") @DefaultValue("-1") String refreshValue) {
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
    model.put("title", "Traces for " + type + " for the last " + minutes + " minutes");
    model.put("version", Constants.VERSION);
    model.put("refresh", refresh);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    model.put("current_date", new Date().toString().replace(" ", "&nbsp;"));
    model.put("num_logs", logs.size());
    model.put("logs_have_error", logsHaveError);
    model.put("num_problems", numProblems);
    model.put("is_ssl", false);
    model.put("type", type);
    model.put("minutes", minutes);
    model.put("redirect", redir);

    return new Viewable("listType.ftl", model);
  }

  @Path("/show")
  @GET
  public Viewable getID(@QueryParam("id") String id, @CookieParam("page.refresh.rate ") @DefaultValue("-1") String refreshValue) {
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
    model.put("title", "Trace ID " + id);
    model.put("version", Constants.VERSION);
    model.put("refresh", refresh);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    model.put("current_date", new Date().toString().replace(" ", "&nbsp;"));
    model.put("num_logs", logs.size());
    model.put("logs_have_error", logsHaveError);
    model.put("num_problems", numProblems);
    model.put("is_ssl", false);
    model.put("id", id);
    model.put("redirect", redir);

    return new Viewable("show.ftl", model);
  }
}
