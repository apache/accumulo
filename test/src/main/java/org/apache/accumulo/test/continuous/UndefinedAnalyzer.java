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
package org.apache.accumulo.test.continuous;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.BatchScannerOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.cli.ClientOnDefaultTable;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * BUGS This code does not handle the fact that these files could include log events from previous months. It therefore it assumes all dates are in the current
 * month. One solution might be to skip log files that haven't been touched in the last month, but that doesn't prevent newer files that have old dates in them.
 *
 */
public class UndefinedAnalyzer {

  static class UndefinedNode {

    public UndefinedNode(String undef2, String ref2) {
      this.undef = undef2;
      this.ref = ref2;
    }

    String undef;
    String ref;
  }

  static class IngestInfo {

    Map<String,TreeMap<Long,Long>> flushes = new HashMap<String,TreeMap<Long,Long>>();

    public IngestInfo(String logDir) throws Exception {
      File dir = new File(logDir);
      File[] ingestLogs = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith("ingest.out");
        }
      });

      for (File log : ingestLogs) {
        parseLog(log);
      }
    }

    private void parseLog(File log) throws Exception {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(log), UTF_8));
      String line;
      TreeMap<Long,Long> tm = null;
      try {
        while ((line = reader.readLine()) != null) {
          if (!line.startsWith("UUID"))
            continue;
          String[] tokens = line.split("\\s");
          String time = tokens[1];
          String uuid = tokens[2];

          if (flushes.containsKey(uuid)) {
            System.err.println("WARN Duplicate uuid " + log);
            return;
          }

          tm = new TreeMap<Long,Long>(Collections.reverseOrder());
          tm.put(0l, Long.parseLong(time));
          flushes.put(uuid, tm);
          break;

        }
        if (tm == null) {
          System.err.println("WARN Bad ingest log " + log);
          return;
        }

        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split("\\s");

          if (!tokens[0].equals("FLUSH"))
            continue;

          String time = tokens[1];
          String count = tokens[4];

          tm.put(Long.parseLong(count), Long.parseLong(time));
        }
      } finally {
        reader.close();
      }
    }

    Iterator<Long> getTimes(String uuid, long count) {
      TreeMap<Long,Long> tm = flushes.get(uuid);

      if (tm == null)
        return null;

      return tm.tailMap(count).values().iterator();
    }
  }

  static class TabletAssignment {
    String tablet;
    String endRow;
    String prevEndRow;
    String server;
    long time;

    TabletAssignment(String tablet, String er, String per, String server, long time) {
      this.tablet = tablet;
      this.endRow = er;
      this.prevEndRow = per;
      this.server = server;
      this.time = time;
    }

    public boolean contains(String row) {
      return prevEndRow.compareTo(row) < 0 && endRow.compareTo(row) >= 0;
    }
  }

  static class TabletHistory {

    List<TabletAssignment> assignments = new ArrayList<TabletAssignment>();

    TabletHistory(String tableId, String acuLogDir) throws Exception {
      File dir = new File(acuLogDir);
      File[] masterLogs = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.matches("master.*debug.log.*");
        }
      });

      SimpleDateFormat sdf = new SimpleDateFormat("dd HH:mm:ss,SSS yyyy MM");
      String currentYear = (Calendar.getInstance().get(Calendar.YEAR)) + "";
      String currentMonth = (Calendar.getInstance().get(Calendar.MONTH) + 1) + "";

      for (File masterLog : masterLogs) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(masterLog), UTF_8));
        String line;
        try {
          while ((line = reader.readLine()) != null) {
            if (line.contains("TABLET_LOADED")) {
              String[] tokens = line.split("\\s+");
              String tablet = tokens[8];
              String server = tokens[10];

              int pos1 = -1;
              int pos2 = -1;
              int pos3 = -1;

              for (int i = 0; i < tablet.length(); i++) {
                if (tablet.charAt(i) == '<' || tablet.charAt(i) == ';') {
                  if (pos1 == -1) {
                    pos1 = i;
                  } else if (pos2 == -1) {
                    pos2 = i;
                  } else {
                    pos3 = i;
                  }
                }
              }

              if (pos1 > 0 && pos2 > 0 && pos3 == -1) {
                String tid = tablet.substring(0, pos1);
                String endRow = tablet.charAt(pos1) == '<' ? "8000000000000000" : tablet.substring(pos1 + 1, pos2);
                String prevEndRow = tablet.charAt(pos2) == '<' ? "" : tablet.substring(pos2 + 1);
                if (tid.equals(tableId)) {
                  // System.out.println(" "+server+" "+tid+" "+endRow+" "+prevEndRow);
                  Date date = sdf.parse(tokens[0] + " " + tokens[1] + " " + currentYear + " " + currentMonth);
                  // System.out.println(" "+date);

                  assignments.add(new TabletAssignment(tablet, endRow, prevEndRow, server, date.getTime()));

                }
              } else if (!tablet.startsWith("!0")) {
                System.err.println("Cannot parse tablet " + tablet);
              }

            }
          }
        } finally {
          reader.close();
        }
      }
    }

    TabletAssignment findMostRecentAssignment(String row, long time1, long time2) {

      long latest = Long.MIN_VALUE;
      TabletAssignment ret = null;

      for (TabletAssignment assignment : assignments) {
        if (assignment.contains(row) && assignment.time <= time2 && assignment.time > latest) {
          latest = assignment.time;
          ret = assignment;
        }
      }

      return ret;
    }
  }

  static class Opts extends ClientOnDefaultTable {
    @Parameter(names = "--logdir", description = "directory containing the log files", required = true)
    String logDir;

    Opts() {
      super("ci");
    }
  }

  /**
   * Class to analyze undefined references and accumulo logs to isolate the time/tablet where data was lost.
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    BatchScannerOpts bsOpts = new BatchScannerOpts();
    opts.parseArgs(UndefinedAnalyzer.class.getName(), args, bsOpts);

    List<UndefinedNode> undefs = new ArrayList<UndefinedNode>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split("\\s");
      String undef = tokens[0];
      String ref = tokens[1];

      undefs.add(new UndefinedNode(undef, ref));
    }

    Connector conn = opts.getConnector();
    BatchScanner bscanner = conn.createBatchScanner(opts.getTableName(), opts.auths, bsOpts.scanThreads);
    bscanner.setTimeout(bsOpts.scanTimeout, TimeUnit.MILLISECONDS);
    List<Range> refs = new ArrayList<Range>();

    for (UndefinedNode undefinedNode : undefs)
      refs.add(new Range(new Text(undefinedNode.ref)));

    bscanner.setRanges(refs);

    HashMap<String,List<String>> refInfo = new HashMap<String,List<String>>();

    for (Entry<Key,Value> entry : bscanner) {
      String ref = entry.getKey().getRow().toString();
      List<String> vals = refInfo.get(ref);
      if (vals == null) {
        vals = new ArrayList<String>();
        refInfo.put(ref, vals);
      }

      vals.add(entry.getValue().toString());
    }

    bscanner.close();

    IngestInfo ingestInfo = new IngestInfo(opts.logDir);
    TabletHistory tabletHistory = new TabletHistory(Tables.getTableId(conn.getInstance(), opts.getTableName()), opts.logDir);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    for (UndefinedNode undefinedNode : undefs) {

      List<String> refVals = refInfo.get(undefinedNode.ref);
      if (refVals != null) {
        for (String refVal : refVals) {
          TabletAssignment ta = null;

          String[] tokens = refVal.split(":");

          String uuid = tokens[0];
          String count = tokens[1];

          String t1 = "";
          String t2 = "";

          Iterator<Long> times = ingestInfo.getTimes(uuid, Long.parseLong(count, 16));
          if (times != null) {
            if (times.hasNext()) {
              long time2 = times.next();
              t2 = sdf.format(new Date(time2));
              if (times.hasNext()) {
                long time1 = times.next();
                t1 = sdf.format(new Date(time1));
                ta = tabletHistory.findMostRecentAssignment(undefinedNode.undef, time1, time2);
              }
            }
          }

          if (ta == null)
            System.out.println(undefinedNode.undef + " " + undefinedNode.ref + " " + uuid + " " + t1 + " " + t2);
          else
            System.out.println(undefinedNode.undef + " " + undefinedNode.ref + " " + ta.tablet + " " + ta.server + " " + uuid + " " + t1 + " " + t2);

        }
      } else {
        System.out.println(undefinedNode.undef + " " + undefinedNode.ref);
      }

    }

  }

}
