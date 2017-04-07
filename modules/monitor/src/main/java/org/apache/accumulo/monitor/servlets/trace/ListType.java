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

import java.security.PrivilegedAction;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.monitor.util.celltypes.StringType;
import org.apache.accumulo.tracer.TraceFormatter;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

public class ListType extends Basic {

  private static final long serialVersionUID = 1L;

  String getType(HttpServletRequest req) {
    return getStringParameter(req, "type", "<Unknown>");
  }

  int getMinutes(HttpServletRequest req) {
    return getIntParameter(req, "minutes", Summary.DEFAULT_MINUTES);
  }

  @Override
  public void pageBody(HttpServletRequest req, HttpServletResponse resp, StringBuilder sb) throws Exception {
    final String type = getType(req);
    int minutes = getMinutes(req);
    long endTime = System.currentTimeMillis();
    long startTime = endTime - minutes * 60 * 1000;
    Entry<Scanner,UserGroupInformation> entry = getScanner(sb);
    final Scanner scanner = entry.getKey();
    if (scanner == null) {
      return;
    }
    Range range = new Range(new Text("start:" + Long.toHexString(startTime)), new Text("start:" + Long.toHexString(endTime)));
    scanner.setRange(range);
    final Table trace = new Table("trace", "Traces for " + getType(req));
    trace.addSortableColumn("Start", new ShowTraceLinkType(), "Start Time");
    trace.addSortableColumn("ms", new DurationType(), "Span time");
    trace.addUnsortableColumn("Source", new StringType<String>(), "Service and location");

    if (null != entry.getValue()) {
      entry.getValue().doAs(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          addRows(scanner, type, trace);
          return null;
        }
      });
    } else {
      addRows(scanner, type, trace);
    }

    trace.generate(req, sb);
  }

  private void addRows(Scanner scanner, String type, Table trace) {
    for (Entry<Key,Value> entry : scanner) {
      RemoteSpan span = TraceFormatter.getRemoteSpan(entry);
      if (span.description.equals(type)) {
        trace.addRow(span, Long.valueOf(span.stop - span.start), span.svc + ":" + span.sender);
      }
    }
  }

  @Override
  public String getTitle(HttpServletRequest req) {
    return "Traces for " + getType(req) + " for the last " + getMinutes(req) + " minutes";
  }
}
