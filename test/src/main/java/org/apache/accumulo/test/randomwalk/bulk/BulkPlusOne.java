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
package org.apache.accumulo.test.randomwalk.bulk;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class BulkPlusOne extends BulkImportTest {

  public static final int LOTS = 100000;
  public static final int COLS = 10;
  public static final int HEX_SIZE = (int) Math.ceil(Math.log(LOTS) / Math.log(16));
  public static final String FMT = "r%0" + HEX_SIZE + "x";
  public static final List<Column> COLNAMES = new ArrayList<Column>();
  public static final Text CHECK_COLUMN_FAMILY = new Text("cf");
  static {
    for (int i = 0; i < COLS; i++) {
      COLNAMES.add(new Column(CHECK_COLUMN_FAMILY, new Text(String.format("%03d", i))));
    }
  }
  public static final Text MARKER_CF = new Text("marker");
  static final AtomicLong counter = new AtomicLong();

  private static final Value ONE = new Value("1".getBytes());

  static void bulkLoadLots(Logger log, State state, Value value) throws Exception {
    final Path dir = new Path("/tmp", "bulk_" + UUID.randomUUID().toString());
    final Path fail = new Path(dir.toString() + "_fail");
    final DefaultConfiguration defaultConfiguration = AccumuloConfiguration.getDefaultConfiguration();
    final Random rand = (Random) state.get("rand");
    final FileSystem fs = (FileSystem) state.get("fs");
    fs.mkdirs(fail);
    final int parts = rand.nextInt(10) + 1;

    TreeSet<Integer> startRows = new TreeSet<Integer>();
    startRows.add(0);
    while (startRows.size() < parts)
      startRows.add(rand.nextInt(LOTS));

    List<String> printRows = new ArrayList<String>(startRows.size());
    for (Integer row : startRows)
      printRows.add(String.format(FMT, row));

    String markerColumnQualifier = String.format("%07d", counter.incrementAndGet());
    log.debug("preparing bulk files with start rows " + printRows + " last row " + String.format(FMT, LOTS - 1) + " marker " + markerColumnQualifier);

    List<Integer> rows = new ArrayList<Integer>(startRows);
    rows.add(LOTS);

    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.", i) + RFile.EXTENSION;
      FileSKVWriter f = FileOperations.getInstance().openWriter(fileName, fs, fs.getConf(), defaultConfiguration);
      f.startDefaultLocalityGroup();
      int start = rows.get(i);
      int end = rows.get(i + 1);
      for (int j = start; j < end; j++) {
        Text row = new Text(String.format(FMT, j));
        for (Column col : COLNAMES) {
          f.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), value);
        }
        f.append(new Key(row, MARKER_CF, new Text(markerColumnQualifier)), ONE);
      }
      f.close();
    }
    state.getConnector().tableOperations().importDirectory(Setup.getTableName(), dir.toString(), fail.toString(), true);
    fs.delete(dir, true);
    FileStatus[] failures = fs.listStatus(fail);
    if (failures != null && failures.length > 0) {
      state.set("bulkImportSuccess", "false");
      throw new Exception(failures.length + " failure files found importing files from " + dir);
    }
    fs.delete(fail, true);
    log.debug("Finished bulk import, start rows " + printRows + " last row " + String.format(FMT, LOTS - 1) + " marker " + markerColumnQualifier);
  }

  @Override
  protected void runLater(State state) throws Exception {
    log.info("Incrementing");
    bulkLoadLots(log, state, ONE);
  }

}
