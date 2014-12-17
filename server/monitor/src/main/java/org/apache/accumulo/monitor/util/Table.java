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
package org.apache.accumulo.monitor.util;

import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;

import org.apache.accumulo.monitor.servlets.BasicServlet;
import org.apache.accumulo.monitor.util.celltypes.CellType;
import org.apache.accumulo.monitor.util.celltypes.StringType;

public class Table {
  private String table;
  private String caption;
  private String captionclass;
  private String subcaption;
  private ArrayList<TableColumn<?>> columns;
  private ArrayList<TableRow> rows;
  private boolean hasBegunAddingRows = false;

  public Table(String tableName, String caption) {
    this(tableName, caption, null);
  }

  public Table(String tableName, String caption, String captionClass) {
    this.table = tableName;
    this.caption = caption;
    this.captionclass = captionClass;
    this.subcaption = null;
    this.columns = new ArrayList<TableColumn<?>>();
    this.rows = new ArrayList<TableRow>();
  }

  public synchronized void setSubCaption(String subcaption) {
    this.subcaption = subcaption;
  }

  public synchronized <T> void addColumn(TableColumn<T> column) {
    if (hasBegunAddingRows)
      throw new IllegalStateException("Cannot add more columns newServer rows have been added");
    columns.add(column);
  }

  private synchronized <T> void addColumn(String title, CellType<T> type, String legend, boolean sortable) {
    if (type == null)
      type = new StringType<T>();
    type.setSortable(sortable);
    addColumn(new TableColumn<T>(title, type, legend));
  }

  public synchronized <T> void addUnsortableColumn(String title, CellType<T> type, String legend) {
    addColumn(title, type, legend, false);
  }

  public synchronized <T> void addSortableColumn(String title, CellType<T> type, String legend) {
    addColumn(title, type, legend, true);
  }

  public synchronized void addUnsortableColumn(String title) {
    addUnsortableColumn(title, null, null);
  }

  public synchronized void addSortableColumn(String title) {
    addSortableColumn(title, null, null);
  }

  public synchronized TableRow prepareRow() {
    hasBegunAddingRows = true;
    return new TableRow(columns.size());
  }

  public synchronized void addRow(TableRow row) {
    hasBegunAddingRows = true;
    if (columns.size() != row.size())
      throw new IllegalStateException("Row must be the same size as the columns");
    rows.add(row);
  }

  public synchronized void addRow(Object... cells) {
    TableRow row = prepareRow();
    if (cells.length != columns.size())
      throw new IllegalArgumentException("Argument length not equal to the number of columns");
    for (Object cell : cells)
      row.add(cell);
    addRow(row);
  }

  public synchronized void generate(HttpServletRequest req, StringBuilder sb) {
    String page = req.getRequestURI();
    if (columns.isEmpty())
      throw new IllegalStateException("No columns in table");
    for (TableRow row : rows)
      if (row.size() != columns.size())
        throw new RuntimeException("Each row must have the same number of columns");

    boolean sortAscending = !"false".equals(BasicServlet.getCookieValue(req, "tableSort." + BasicServlet.encode(page) + "." + BasicServlet.encode(table) + "."
        + "sortAsc"));

    int sortCol = -1; // set to first sortable column by default
    int numLegends = 0;
    for (int i = 0; i < columns.size(); ++i) {
      TableColumn<?> col = columns.get(i);
      if (sortCol < 0 && col.getCellType().isSortable())
        sortCol = i;
      if (col.getLegend() != null && !col.getLegend().isEmpty())
        ++numLegends;
    }

    // only get cookie if there is a possibility that it is sortable
    if (sortCol >= 0) {
      String sortColStr = BasicServlet.getCookieValue(req, "tableSort." + BasicServlet.encode(page) + "." + BasicServlet.encode(table) + "." + "sortCol");
      if (sortColStr != null) {
        try {
          int col = Integer.parseInt(sortColStr);
          // only bother if specified column is sortable
          if (!(col < 0 || sortCol >= columns.size()) && columns.get(col).getCellType().isSortable())
            sortCol = col;
        } catch (NumberFormatException e) {
          // ignore improperly formatted user cookie
        }
      }
    }

    boolean showLegend = false;
    if (numLegends > 0) {
      String showStr = BasicServlet.getCookieValue(req, "tableLegend." + BasicServlet.encode(page) + "." + BasicServlet.encode(table) + "." + "show");
      showLegend = showStr != null && Boolean.parseBoolean(showStr);
    }

    sb.append("<div>\n");
    sb.append("<a name='").append(table).append("'>&nbsp;</a>\n");
    sb.append("<table id='").append(table).append("' class='sortable'>\n");
    sb.append("<caption");
    if (captionclass != null && !captionclass.isEmpty())
      sb.append(" class='").append(captionclass).append("'");
    sb.append(">\n");
    if (caption != null && !caption.isEmpty())
      sb.append("<span class='table-caption'>").append(caption).append("</span><br />\n");
    if (subcaption != null && !subcaption.isEmpty())
      sb.append("<span class='table-subcaption'>").append(subcaption).append("</span><br />\n");

    String redir = BasicServlet.currentPage(req);
    if (numLegends > 0) {
      String legendUrl = String.format("/op?action=toggleLegend&redir=%s&page=%s&table=%s&show=%s", redir, page, table, !showLegend);
      sb.append("<a href='").append(legendUrl).append("'>").append(showLegend ? "Hide" : "Show").append("&nbsp;Legend</a>\n");
      if (showLegend)
        sb.append("<div class='left ").append(showLegend ? "show" : "hide").append("'><dl>\n");
    }
    for (int i = 0; i < columns.size(); ++i) {
      TableColumn<?> col = columns.get(i);
      String title = col.getTitle();
      if (rows.size() > 1 && col.getCellType().isSortable()) {
        String url = String.format("/op?action=sortTable&redir=%s&page=%s&table=%s&%s=%s", redir, page, table, sortCol == i ? "asc" : "col",
            sortCol == i ? !sortAscending : i);
        String img = "";
        if (sortCol == i)
          img = String.format("&nbsp;<img width='10px' height='10px' src='/web/%s.gif' alt='%s' />", sortAscending ? "up" : "down", !sortAscending ? "^" : "v");
        col.setTitle(String.format("<a href='%s'>%s%s</a>", url, title, img));
      }
      String legend = col.getLegend();
      if (showLegend && legend != null && !legend.isEmpty())
        sb.append("<dt class='smalltext'><b>").append(title.replace("<br />", "&nbsp;")).append("</b><dd>").append(legend).append("</dd></dt>\n");
    }
    if (showLegend && numLegends > 0)
      sb.append("</dl></div>\n");
    sb.append("</caption>\n");
    sb.append("<tr>");
    boolean first = true;
    for (TableColumn<?> col : columns) {
      String cellValue = col.getTitle() == null ? "" : String.valueOf(col.getTitle()).trim();
      sb.append("<th").append(first ? " class='firstcell'" : "").append(">").append(cellValue.isEmpty() ? "-" : cellValue).append("</th>");
      first = false;
    }
    sb.append("</tr>\n");
    // don't sort if no columns are sortable or if there aren't enough rows
    if (rows.size() > 1 && sortCol > -1) {
      Collections.sort(rows, TableRow.getComparator(sortCol, columns.get(sortCol).getCellType()));
      if (!sortAscending)
        Collections.reverse(rows);
    }
    boolean highlight = true;
    for (TableRow row : rows) {
      for (int i = 0; i < row.size(); ++i) {
        try {
          row.set(i, columns.get(i).getCellType().format(row.get(i)));
        } catch (Exception ex) {
          throw new RuntimeException("Unable to process column " + i, ex);
        }
      }
      row(sb, highlight, columns, row);
      highlight = !highlight;
    }
    if (rows.isEmpty())
      sb.append("<tr><td class='center' colspan='").append(columns.size()).append("'><i>Empty</i></td></tr>\n");
    sb.append("</table>\n</div>\n\n");
  }

  private static void row(StringBuilder sb, boolean highlight, ArrayList<TableColumn<?>> columns, TableRow row) {
    sb.append(highlight ? "<tr class='highlight'>" : "<tr>");
    boolean first = true;
    for (int i = 0; i < row.size(); ++i) {
      String cellValue = String.valueOf(row.get(i)).trim();
      if (cellValue.isEmpty() || cellValue.equals(String.valueOf((Object) null)))
        cellValue = "-";
      sb.append("<td class='").append(first ? "firstcell" : "");
      if (columns.get(i).getCellType().alignment() != null)
        sb.append(first ? " " : "").append(columns.get(i).getCellType().alignment());
      sb.append("'>").append(cellValue).append("</td>");
      first = false;
    }
    sb.append("</tr>\n");
  }

}
