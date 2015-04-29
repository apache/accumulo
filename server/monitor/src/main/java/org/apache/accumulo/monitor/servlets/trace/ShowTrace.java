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
package org.apache.accumulo.monitor.servlets.trace;

import static java.lang.Math.min;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.monitor.servlets.BasicServlet;
import org.apache.accumulo.tracer.SpanTree;
import org.apache.accumulo.tracer.SpanTreeVisitor;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceFormatter;
import org.apache.accumulo.tracer.thrift.Annotation;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

public class ShowTrace extends Basic {

  private static final long serialVersionUID = 1L;
  private static final String checkboxIdSuffix = "_checkbox";
  private static final String pageLoadFunctionName = "pageload";

  String getTraceId(HttpServletRequest req) {
    return getStringParameter(req, "id", null);
  }

  @Override
  public String getTitle(HttpServletRequest req) {
    String id = getTraceId(req);
    if (id == null)
      return "No trace id specified";
    return "Trace ID " + id;
  }

  private long addSpans(Scanner scanner, SpanTree tree, long start) {
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      tree.addNode(span);
      start = min(start, span.start);
    }
    return start;
  }

  @Override
  public void pageBody(HttpServletRequest req, HttpServletResponse resp, final StringBuilder sb) throws Exception {
    String id = getTraceId(req);
    if (id == null) {
      return;
    }
    Entry<Scanner,UserGroupInformation> entry = getScanner(sb);
    final Scanner scanner = entry.getKey();
    if (scanner == null) {
      return;
    }
    Range range = new Range(new Text(id));
    scanner.setRange(range);
    final SpanTree tree = new SpanTree();
    long start;
    if (null != entry.getValue()) {
      start = entry.getValue().doAs(new PrivilegedAction<Long>() {
        @Override
        public Long run() {
          return addSpans(scanner, tree, Long.MAX_VALUE);
        }
      });
    } else {
      start = addSpans(scanner, tree, Long.MAX_VALUE);
    }
    sb.append("<style>\n");
    sb.append(" td.right { text-align: right }\n");
    sb.append(" table.indent { position: relative; left: 10% }\n");
    sb.append(" td.left { text-align: left }\n");
    sb.append("</style>\n");
    sb.append("<script language='javascript'>\n");
    sb.append("function toggle(id) {\n");
    sb.append(" var elt = document.getElementById(id);\n");
    sb.append(" if (elt.style.display=='none') {\n");
    sb.append("    elt.style.display='table-row';\n");
    sb.append(" } else { \n");
    sb.append("    elt.style.display='none';\n ");
    sb.append(" }\n");
    sb.append("}\n");

    sb.append("function ").append(pageLoadFunctionName).append("() {\n");
    sb.append("  var checkboxes = document.getElementsByTagName('input');\n");
    sb.append("  for (var i = 0; i < checkboxes.length; i++) {\n");
    sb.append("    if (checkboxes[i].checked) {\n");
    sb.append("      var idSuffixOffset = checkboxes[i].id.indexOf('").append(checkboxIdSuffix).append("');\n");
    sb.append("      var id = checkboxes[i].id.substring(0, idSuffixOffset);\n");
    sb.append("      document.getElementById(id).style.display='table-row';\n");
    sb.append("    }\n");
    sb.append("  }\n");
    sb.append("}\n");

    sb.append("</script>\n");
    sb.append("<div>");
    sb.append("<table><caption>");
    sb.append(String.format("<span class='table-caption'>Trace %s started at<br>%s</span></caption>", id, dateString(start)));
    sb.append("<tr><th>Time</th><th>Start</th><th>Service@Location</th><th>Name</th><th>Addl Data</th></tr>");

    final long finalStart = start;
    Set<Long> visited = tree.visit(new SpanTreeVisitor() {
      @Override
      public void visit(int level, RemoteSpan parent, RemoteSpan node, Collection<RemoteSpan> children) {
        appendRow(sb, level, node, finalStart);
      }
    });
    tree.nodes.keySet().removeAll(visited);
    if (!tree.nodes.isEmpty()) {
      sb.append("<tr><td colspan=10>The following spans are not rooted (probably due to a parent span of length 0ms):<td></tr>\n");
      for (RemoteSpan span : TraceDump.sortByStart(tree.nodes.values())) {
        appendRow(sb, 0, span, finalStart);
      }
    }
    sb.append("</table>\n");
    sb.append("</div>\n");
  }

  private static void appendRow(StringBuilder sb, int level, RemoteSpan node, long finalStart) {
    sb.append("<tr>\n");
    sb.append(String.format("<td class='right'>%d+</td><td class='left'>%d</td>%n", node.stop - node.start, node.start - finalStart));
    sb.append(String.format("<td style='text-indent: %dpx'>%s@%s</td>%n", level * 10, node.svc, node.sender));
    sb.append("<td>" + node.description + "</td>");
    boolean hasData = node.data != null && !node.data.isEmpty();
    boolean hasAnnotations = node.annotations != null && !node.annotations.isEmpty();
    if (hasData || hasAnnotations) {
      String hexSpanId = Long.toHexString(node.spanId);
      sb.append("<td><input type='checkbox' id=\"");
      sb.append(hexSpanId);
      sb.append(checkboxIdSuffix);
      sb.append("\" onclick='toggle(\"" + Long.toHexString(node.spanId) + "\")'></td>\n");
    } else {
      sb.append("<td></td>\n");
    }
    sb.append("</tr>\n");
    sb.append("<tr id='" + Long.toHexString(node.spanId) + "' style='display:none'>");
    sb.append("<td colspan='5'>\n");
    if (hasData || hasAnnotations) {
      sb.append("  <table class='indent,noborder'>\n");
      if (hasData) {
        sb.append("  <tr><th>Key</th><th>Value</th></tr>\n");
        for (Entry<String,String> entry : node.data.entrySet()) {
          sb.append("  <tr><td>" + BasicServlet.sanitize(entry.getKey()) + "</td>");
          sb.append("<td>" + BasicServlet.sanitize(entry.getValue()) + "</td></tr>\n");
        }
      }
      if (hasAnnotations) {
        sb.append("  <tr><th>Annotation</th><th>Time Offset</th></tr>\n");
        for (Annotation entry : node.annotations) {
          sb.append("  <tr><td>" + BasicServlet.sanitize(entry.getMsg()) + "</td>");
          sb.append(String.format("<td>%d</td></tr>\n", entry.getTime() - finalStart));
        }
      }
      sb.append("  </table>");
    }
    sb.append("</td>\n");
    sb.append("</tr>\n");
  }

  @Override
  protected String getBodyAttributes() {
    return " onload=\"" + pageLoadFunctionName + "()\" ";
  }
}
