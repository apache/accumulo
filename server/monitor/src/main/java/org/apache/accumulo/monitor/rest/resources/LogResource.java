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
package org.apache.accumulo.monitor.rest.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.monitor.rest.api.LogEvent;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.spi.LoggingEvent;

/**
 * 
 */
@Path("/logs")
@Produces(MediaType.APPLICATION_JSON)
public class LogResource {

  @GET
  public List<LogEvent> getRecentLogs() {
    List<DedupedLogEvent> dedupedLogEvents = LogService.getInstance().getEvents();
    ArrayList<LogEvent> logEvents = new ArrayList<LogEvent>(dedupedLogEvents.size());

    final StringBuilder msg = new StringBuilder(64);
    for (DedupedLogEvent dev : dedupedLogEvents) {
      msg.setLength(0);
      final LoggingEvent ev = dev.getEvent();
      Object application = ev.getMDC("application");
      if (application == null)
        application = "";

      msg.append(ev.getMessage().toString());
      if (ev.getThrowableStrRep() != null)
        for (String line : ev.getThrowableStrRep())
          msg.append("\n\t").append(line);

      logEvents.add(new LogEvent(ev.getTimeStamp(), application, dev.getCount(), ev.getLevel().toString(), msg.toString().trim()));
    }

    return logEvents;
  }
}
