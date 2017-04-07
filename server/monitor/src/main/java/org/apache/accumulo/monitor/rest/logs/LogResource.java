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
package org.apache.accumulo.monitor.rest.logs;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * Responsible for generating a new log JSON object
 *
 * @since 2.0.0
 *
 */
@Path("/logs")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class LogResource {

  /**
   * Generates log event list as a JSON object
   *
   * @return log event array
   */
  @GET
  public List<LogEvent> getRecentLogs() {
    List<DedupedLogEvent> dedupedLogEvents = LogService.getInstance().getEvents();
    ArrayList<LogEvent> logEvents = new ArrayList<>(dedupedLogEvents.size());

    for (DedupedLogEvent dev : dedupedLogEvents) {
      LoggingEvent ev = dev.getEvent();
      Object application = ev.getMDC("application");
      if (application == null)
        application = "";
      String msg = ev.getMessage().toString();
      StringBuilder text = new StringBuilder();
      for (int i = 0; i < msg.length(); i++) {
        char c = msg.charAt(i);
        int type = Character.getType(c);
        boolean notPrintable = type == Character.UNASSIGNED || type == Character.LINE_SEPARATOR || type == Character.NON_SPACING_MARK
            || type == Character.PRIVATE_USE;
        text.append(notPrintable ? '?' : c);
      }
      StringBuilder builder = new StringBuilder(text.toString());
      if (ev.getThrowableStrRep() != null)
        for (String line : ev.getThrowableStrRep())
          builder.append("\n\t").append(line);
      msg = sanitize(builder.toString().trim());

      // Add a new log event to the list
      logEvents.add(new LogEvent(ev.getTimeStamp(), application, dev.getCount(), ev.getLevel().toString(), msg.toString().trim()));
    }
    return logEvents;
  }

  private String sanitize(String xml) {
    return xml.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  }

  /**
   * REST call to clear the logs
   */
  @POST
  public void clearLogs() {
    LogService.getInstance().clear();
  }
}
