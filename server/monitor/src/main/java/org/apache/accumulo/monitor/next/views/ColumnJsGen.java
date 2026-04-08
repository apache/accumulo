/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.next.views;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.monitor.next.views.ServersView.ADDR_COL_NAME;
import static org.apache.accumulo.monitor.next.views.ServersView.RG_COL_NAME;
import static org.apache.accumulo.monitor.next.views.ServersView.TIME_COL_NAME;
import static org.apache.accumulo.monitor.next.views.ServersView.TYPE_COL_NAME;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.util.LazySingletons;

/**
 * This class generates a map of metric name to column information for the Monitor
 */
public class ColumnJsGen {

  public static record ColumnInformation(String header, String description, String classes) {
  };

  private static void printHeader(PrintStream out) {
    final String hdr = """
        /*
        * Licensed to the Apache Software Foundation (ASF) under one
        * or more contributor license agreements.  See the NOTICE file
        * distributed with this work for additional information
        * regarding copyright ownership.  The ASF licenses this file
        * to you under the Apache License, Version 2.0 (the
        * "License"); you may not use this file except in compliance
        * with the License.  You may obtain a copy of the License at
        *
        *   https://www.apache.org/licenses/LICENSE-2.0
        *
        * Unless required by applicable law or agreed to in writing,
        * software distributed under the License is distributed on an
        * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        * KIND, either express or implied.  See the License for the
        * specific language governing permissions and limitations
        * under the License.
        */

        "use strict";

        const COLUMN_MAP = new Map([""";
    out.println(hdr);
  }

  private static void printMetrics(PrintStream out) {

    // Put in tree map to sort metrics by name in the output
    Map<String,ColumnInformation> output = new TreeMap<>();
    for (Metric m : Metric.values()) {
      output.put(m.getName(), new ColumnInformation(m.getColumnHeader(), m.getColumnDescription(),
          m.getColumnClasses()));
    }

    // Add non-metric columns
    output.put(ADDR_COL_NAME, new ColumnInformation(ADDR_COL_NAME, "Server Address", "firstcell"));
    output.put(RG_COL_NAME,
        new ColumnInformation(RG_COL_NAME, "Resource Group Name", "resource-group"));
    output.put(TIME_COL_NAME,
        new ColumnInformation(TIME_COL_NAME, "Last Contact Time", "duration"));
    output.put(TYPE_COL_NAME,
        new ColumnInformation(TYPE_COL_NAME, "Server Process Type", "server-type"));

    final Set<String> keys = output.keySet();
    final int numKeys = keys.size();
    int counter = 0;
    for (String key : keys) {
      counter++;
      out.println("  [\"%s\", %s]%s".formatted(key,
          LazySingletons.GSON.get().toJson(output.get(key)), counter == numKeys ? "" : ","));

    }
  }

  private static void printFooter(PrintStream out) {
    final String footer = """
        ]);
        """;
    out.println(footer);
  }

  public static void main(String args[]) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException(
          "Usage: " + ColumnJsGen.class.getName() + " <output_filename>");
    }
    try (var printStream = new PrintStream(args[0], UTF_8)) {
      printHeader(printStream);
      printMetrics(printStream);
      printFooter(printStream);
    }
  }

}
