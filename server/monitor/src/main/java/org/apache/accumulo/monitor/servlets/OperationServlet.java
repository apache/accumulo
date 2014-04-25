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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.log4j.Logger;

public class OperationServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Operations";
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String redir = null;
    try {
      String operation = req.getParameter("action");
      redir = req.getParameter("redir");

      if (operation != null) {
        for (Class<?> subclass : OperationServlet.class.getClasses()) {
          Object t;
          try {
            t = subclass.newInstance();
          } catch (Exception e) {
            continue;
          }
          if (t instanceof WebOperation) {
            WebOperation op = (WebOperation) t;
            if (op.getClass().getSimpleName().equalsIgnoreCase(operation + "Operation")) {
              op.execute(req, resp, log);
              break;
            }
          }
        }
      }
    } catch (Throwable t) {
      log.error(t, t);
    } finally {
      try {
        redir = redir == null ? "" : redir;
        redir = redir.startsWith("/") ? redir.substring(1) : redir;
        resp.sendRedirect("/" + redir); // this makes findbugs happy
        resp.flushBuffer();
      } catch (Throwable t) {
        log.error(t, t);
      }
    }
  }

  private interface WebOperation {
    void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) throws Exception;
  }

  public static class RefreshOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) {
      String value = req.getParameter("value");
      BasicServlet.setCookie(resp, "page.refresh.rate", value == null ? "5" : BasicServlet.encode(value));
    }
  }

  public static class ClearLogOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) {
      LogService.getInstance().clear();
    }
  }

  public static class ClearTableProblemsOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) {
      String table = req.getParameter("table");
      try {
        ProblemReports.getInstance().deleteProblemReports(table);
      } catch (Exception e) {
        log.error("Failed to delete problem reports for table " + table, e);
      }
    }
  }

  public static class ClearProblemOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) {
      String table = req.getParameter("table");
      String resource = req.getParameter("resource");
      String ptype = req.getParameter("ptype");
      try {
        ProblemReports.getInstance().deleteProblemReport(table, ProblemType.valueOf(ptype), resource);
      } catch (Exception e) {
        log.error("Failed to delete problem reports for table " + table, e);
      }
    }
  }

  public static class SortTableOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) throws IOException {
      String page = req.getParameter("page");
      String table = req.getParameter("table");
      String asc = req.getParameter("asc");
      String col = req.getParameter("col");
      if (table == null || page == null || (asc == null && col == null))
        return;
      page = BasicServlet.encode(page);
      table = BasicServlet.encode(table);
      if (asc == null) {
        col = BasicServlet.encode(col);
        BasicServlet.setCookie(resp, "tableSort." + page + "." + table + "." + "sortCol", col);
      } else {
        asc = BasicServlet.encode(asc);
        BasicServlet.setCookie(resp, "tableSort." + page + "." + table + "." + "sortAsc", asc);
      }
    }
  }

  public static class ToggleLegendOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) throws Exception {
      String page = req.getParameter("page");
      String table = req.getParameter("table");
      String show = req.getParameter("show");
      if (table == null || page == null || show == null)
        return;
      page = BasicServlet.encode(page);
      table = BasicServlet.encode(table);
      show = BasicServlet.encode(show);
      BasicServlet.setCookie(resp, "tableLegend." + page + "." + table + "." + "show", show);
    }
  }

  public static class ClearDeadServerOperation implements WebOperation {
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, Logger log) {
      String server = req.getParameter("server");
      Instance inst = HdfsZooInstance.getInstance();
      // a dead server should have a uniq address: a logger or tserver
      DeadServerList obit = new DeadServerList(ZooUtil.getRoot(inst) + Constants.ZDEADTSERVERS);
      obit.delete(server);
    }
  }
}
