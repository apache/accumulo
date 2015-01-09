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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.TableRow;
import org.apache.accumulo.monitor.util.celltypes.CellType;
import org.apache.accumulo.monitor.util.celltypes.DateTimeType;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.monitor.util.celltypes.StringType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;

public class ProblemServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Per-Table Problem Report";
  }

  @Override
  protected void pageBody(final HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) {
    Map<String,String> tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
    doProblemSummary(req, sb, tidToNameMap);
    doProblemDetails(req, sb, req.getParameter("table"), tidToNameMap);
  }

  private static void doProblemSummary(final HttpServletRequest req, StringBuilder sb, final Map<String,String> tidToNameMap) {
    if (Monitor.getProblemSummary().isEmpty() && Monitor.getProblemException() == null)
      return;

    Table problemSummary = new Table("problemSummary", "Problem&nbsp;Summary", "error");
    problemSummary.addSortableColumn("Table", new TableProblemLinkType(tidToNameMap), null);
    for (ProblemType type : ProblemType.values())
      problemSummary.addSortableColumn(type.name(), new NumberType<Integer>(), null);
    problemSummary.addUnsortableColumn("Operations", new ClearTableProblemsLinkType(req, tidToNameMap), null);

    if (Monitor.getProblemException() != null) {
      StringBuilder cell = new StringBuilder();
      cell.append("<b>Failed to obtain problem reports</b> : " + Monitor.getProblemException().getMessage());
      Throwable cause = Monitor.getProblemException().getCause();
      while (cause != null) {
        if (cause.getMessage() != null)
          cell.append("<br />\n caused by : " + cause.getMessage());
        cause = cause.getCause();
      }
      problemSummary.setSubCaption(cell.toString());
    } else {
      for (Entry<String,Map<ProblemType,Integer>> entry : Monitor.getProblemSummary().entrySet()) {
        TableRow row = problemSummary.prepareRow();
        row.add(entry.getKey());
        for (ProblemType pt : ProblemType.values()) {
          Integer pcount = entry.getValue().get(pt);
          row.add(pcount == null ? 0 : pcount);
        }
        row.add(entry.getKey());
        problemSummary.addRow(row);
      }
    }
    problemSummary.generate(req, sb);
  }

  private static void doProblemDetails(final HttpServletRequest req, StringBuilder sb, String tableId, Map<String,String> tidToNameMap) {

    if (Monitor.getProblemException() != null)
      return;

    ArrayList<ProblemReport> problemReports = new ArrayList<ProblemReport>();
    Iterator<ProblemReport> iter = tableId == null ? ProblemReports.getInstance().iterator() : ProblemReports.getInstance().iterator(tableId);
    while (iter.hasNext())
      problemReports.add(iter.next());
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss zzz");
    Table problemTable = new Table("problemDetails", "Problem&nbsp;Details", "error");
    problemTable.setSubCaption("Problems identified with tables.");
    problemTable.addSortableColumn("Table", new TableProblemLinkType(tidToNameMap), null);
    problemTable.addSortableColumn("Problem&nbsp;Type");
    problemTable.addSortableColumn("Server");
    problemTable.addSortableColumn("Time", new DateTimeType(sdf), null);
    problemTable.addSortableColumn("Resource");
    problemTable.addSortableColumn("Exception");
    problemTable.addUnsortableColumn("Operations", new ClearProblemLinkType(req), null);
    for (ProblemReport pr : problemReports) {

      TableRow row = problemTable.prepareRow();
      row.add(pr.getTableName());
      row.add(pr.getProblemType().name());
      row.add(pr.getServer());
      row.add(pr.getTime());
      row.add(pr.getResource());
      row.add(pr.getException());
      row.add(pr);
      problemTable.addRow(row);
    }
    problemTable.generate(req, sb);
  }

  private static class TableProblemLinkType extends StringType<String> {
    private Map<String,String> tidToNameMap;

    public TableProblemLinkType(Map<String,String> tidToNameMap) {
      this.tidToNameMap = tidToNameMap;
    }

    @Override
    public String format(Object obj) {
      if (obj == null)
        return "-";
      String table = String.valueOf(obj);
      return String.format("<a href='/problems?table=%s'>%s</a>", encode(table), encode((Tables.getPrintableTableNameFromId(tidToNameMap, table))));
    }
  }

  private static class ClearTableProblemsLinkType extends StringType<String> {
    private HttpServletRequest req;
    private Map<String,String> tidToNameMap;

    public ClearTableProblemsLinkType(HttpServletRequest req, Map<String,String> tidToNameMap) {
      this.req = req;
      this.tidToNameMap = tidToNameMap;
    }

    @Override
    public String alignment() {
      return "right";
    }

    @Override
    public String format(Object obj) {
      if (obj == null)
        return "-";
      String table = String.valueOf(obj);
      return String.format("<a href='/op?table=%s&action=clearTableProblems&redir=%s'>clear ALL %s problems</a>", encode(table), currentPage(req),
          Tables.getPrintableTableNameFromId(tidToNameMap, table));
    }
  }

  private static class ClearProblemLinkType extends CellType<ProblemReport> {
    private HttpServletRequest req;

    public ClearProblemLinkType(HttpServletRequest req) {
      this.req = req;
    }

    @Override
    public String alignment() {
      return "right";
    }

    @Override
    public String format(Object obj) {
      if (obj == null)
        return "-";
      ProblemReport p = (ProblemReport) obj;
      return String.format("<a href='/op?table=%s&action=clearProblem&redir=%s&resource=%s&ptype=%s'>clear this problem</a>", encode(p.getTableName()),
          currentPage(req), encode(p.getResource()), encode(p.getProblemType().name()));
    }

    @Override
    public int compare(ProblemReport o1, ProblemReport o2) {
      return 0;
    }
  }

}
