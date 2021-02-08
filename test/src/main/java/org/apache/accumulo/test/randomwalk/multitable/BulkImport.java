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
package org.apache.accumulo.test.randomwalk.multitable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class BulkImport extends Test {

  public static final int LOTS = 100000;
  public static final int COLS = 10;
  public static final List<Column> COLNAMES = new ArrayList<>();
  public static final Text CHECK_COLUMN_FAMILY = new Text("cf");
  static {
    for (int i = 0; i < COLS; i++) {
      COLNAMES.add(new Column(CHECK_COLUMN_FAMILY, new Text(String.format("%03d", i))));
    }
  }
  public static final Text MARKER_CF = new Text("marker");
  static final AtomicLong counter = new AtomicLong();

  private static final Value ONE = new Value("1".getBytes());

  public void visit(final State state, final Environment env, Properties props) throws Exception {
    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");

    if (tables.isEmpty()) {
      log.debug("No tables to ingest into");
      return;
    }

    Random rand = new Random();
    String tableName = tables.get(rand.nextInt(tables.size()));

    String uuid = UUID.randomUUID().toString();
    final Path dir = new Path("/tmp", "bulk_" + uuid);
    final Path fail = new Path(dir.toString() + "_fail");
    final DefaultConfiguration defaultConfiguration =
        AccumuloConfiguration.getDefaultConfiguration();
    final FileSystem fs = (FileSystem) state.get("fs");
    fs.mkdirs(fail);
    final int parts = rand.nextInt(10) + 1;

    TreeSet<String> rows = new TreeSet<>();
    for (int i = 0; i < LOTS; i++)
      rows.add(uuid + String.format("__%05d", i));

    String markerColumnQualifier = String.format("%07d", counter.incrementAndGet());
    log.debug("Preparing bulk import to " + tableName + " start: " + rows.first() + " last: "
        + rows.last() + " marker: " + markerColumnQualifier);

    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.", i) + RFile.EXTENSION;
      FileSKVWriter f = FileOperations.getInstance().newWriterBuilder()
          .forFile(fileName, fs, fs.getConf()).withTableConfiguration(defaultConfiguration).build();
      f.startDefaultLocalityGroup();
      for (String r : rows) {
        Text row = new Text(r);
        for (Column col : COLNAMES) {
          f.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), ONE);
        }
        f.append(new Key(row, MARKER_CF, new Text(markerColumnQualifier)), ONE);
      }
      f.close();
    }
    env.getConnector().tableOperations().importDirectory(tableName, dir.toString(), fail.toString(),
        true);
    fs.delete(dir, true);
    FileStatus[] failures = fs.listStatus(fail);
    if (failures != null && failures.length > 0) {
      state.set("bulkImportSuccess", "false");
      throw new Exception(failures.length + " failure files found importing files from " + dir);
    }
    fs.delete(fail, true);
    log.debug("Finished bulk import to " + tableName + " start: " + rows.first() + " last: "
        + rows.last() + " marker " + markerColumnQualifier);
  }

}
