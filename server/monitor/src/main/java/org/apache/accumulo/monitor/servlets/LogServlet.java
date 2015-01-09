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
package org.apache.accumulo.monitor.servlets;

import java.text.SimpleDateFormat;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.celltypes.DateTimeType;
import org.apache.accumulo.monitor.util.celltypes.StringType;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

public class LogServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Recent Logs";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    boolean clear = true;
    SimpleDateFormat fmt = new SimpleDateFormat("dd HH:mm:ss,SSSS");
    Table logTable = new Table("logTable", "Recent&nbsp;Logs");
    logTable.addSortableColumn("Time", new DateTimeType(fmt), null);
    logTable.addSortableColumn("Application");
    logTable.addSortableColumn("Count");
    logTable.addSortableColumn("Level", new LogLevelType(), null);
    logTable.addSortableColumn("Message");
    for (DedupedLogEvent dev : LogService.getInstance().getEvents()) {
      clear = false;
      LoggingEvent ev = dev.getEvent();
      Object application = ev.getMDC("application");
      if (application == null)
        application = "";
      String msg = ev.getMessage().toString();
      StringBuilder text = new StringBuilder();
      for (int i = 0; i < msg.length(); i++) {
        char c = msg.charAt(i);
        switch (Character.getType(c)) {
          case Character.UNASSIGNED:
          case Character.LINE_SEPARATOR:
          case Character.NON_SPACING_MARK:
          case Character.PRIVATE_USE:
            c = '?';
          default:
            text.append(c);
        }

      }
      StringBuilder builder = new StringBuilder(text.toString());
      if (ev.getThrowableStrRep() != null)
        for (String line : ev.getThrowableStrRep())
          builder.append("\n\t").append(line);
      msg = sanitize(builder.toString().trim());
      msg = "<pre class='logevent'>" + msg + "</pre>";
      logTable.addRow(ev.getTimeStamp(), application, dev.getCount(), ev.getLevel(), msg);
    }
    if (!clear)
      logTable.setSubCaption("<a href='/op?action=clearLog&redir=" + currentPage(req) + "'>Clear&nbsp;All&nbsp;Events</a>");
    logTable.generate(req, sb);
    if (!clear)
      sb.append("<div class='center'><a href='/op?action=clearLog&redir=").append(currentPage(req)).append("'>Clear&nbsp;All&nbsp;Events</a></div>\n");
  }

  private static class LogLevelType extends StringType<Level> {
    @Override
    public String alignment() {
      return "center";
    }

    @Override
    public String format(Object obj) {
      if (obj == null)
        return "-";
      Level l = (Level) obj;
      if (l.equals(Level.ERROR) || l.equals(Level.FATAL))
        return "<div class='error'>" + l.toString() + "</div>";
      else if (l.equals(Level.WARN))
        return "<div class='warning'>" + l.toString() + "</div>";
      else
        return l.toString();
    }
  }
}
