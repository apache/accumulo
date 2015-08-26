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
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.TableRow;
import org.apache.accumulo.monitor.util.celltypes.BulkImportStateType;
import org.apache.accumulo.monitor.util.celltypes.DurationType;
import org.apache.accumulo.monitor.util.celltypes.PreciseNumberType;
import org.apache.accumulo.monitor.util.celltypes.TServerLinkType;

public class BulkImportServlet extends BasicServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Bulk Imports";
  }

  static private long duration(long start) {
    return (System.currentTimeMillis() - start) / 1000L;
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws IOException {
    Table table = new Table("masterBulkImportStatus", "Bulk&nbsp;Import&nbsp;Status");
    table.addSortableColumn("Directory");
    table.addSortableColumn("Age", new DurationType(0l, 5 * 60 * 1000l), "The age the import.");
    table.addSortableColumn("State", new BulkImportStateType(), "The current state of the bulk import");
    for (BulkImportStatus bulk : Monitor.getMmi().bulkImports) {
      TableRow row = table.prepareRow();
      row.add(bulk.filename);
      row.add(duration(bulk.startTime));
      row.add(bulk.state);
      table.addRow(row);
    }
    table.generate(req, sb);

    table = new Table("bulkImportStatus", "TabletServer&nbsp;Bulk&nbsp;Import&nbsp;Status");
    table.addSortableColumn("Server", new TServerLinkType(), null);
    table.addSortableColumn("#", new PreciseNumberType(0, 20, 0, 100), "Number of imports presently running");
    table.addSortableColumn("Oldest&nbsp;Age", new DurationType(0l, 5 * 60 * 1000l), "The age of the oldest import running on this server.");
    for (TabletServerStatus tserverInfo : Monitor.getMmi().getTServerInfo()) {
      TableRow row = table.prepareRow();
      row.add(tserverInfo);
      List<BulkImportStatus> stats = tserverInfo.bulkImports;
      if (stats != null) {
        row.add(stats.size());
        long oldest = Long.MAX_VALUE;
        for (BulkImportStatus bulk : stats) {
          oldest = Math.min(oldest, bulk.startTime);
        }
        if (oldest != Long.MAX_VALUE) {
          row.add(duration(oldest));
        } else {
          row.add(0L);
        }
      } else {
        row.add(0);
        row.add(0L);
      }
      table.addRow(row);
    }
    table.generate(req, sb);
  }

}
