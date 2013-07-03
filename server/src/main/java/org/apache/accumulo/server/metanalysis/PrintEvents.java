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
package org.apache.accumulo.server.metanalysis;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Looks up and prints mutations indexed by IndexMeta
 */
public class PrintEvents {
  
  static class Opts extends ClientOpts {
    @Parameter(names = {"-t", "--tableId"}, description = "table id", required = true)
    String tableId;
    @Parameter(names = {"-e", "--endRow"}, description = "end row")
    String endRow;
    @Parameter(names = {"-t", "--time"}, description = "time, in milliseconds", required = true)
    long time;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(PrintEvents.class.getName(), args);
    
    Connector conn = opts.getConnector();
    
    printEvents(conn, opts.tableId, opts.endRow, opts.time);
  }
  
  /**
   * @param conn
   * @param tablePrefix
   * @param tableId
   * @param endRow
   * @param time
   */
  private static void printEvents(Connector conn, String tableId, String endRow, Long time) throws Exception {
    Scanner scanner = conn.createScanner("tabletEvents", new Authorizations());
    String metaRow = tableId + (endRow == null ? "<" : ";" + endRow);
    scanner.setRange(new Range(new Key(metaRow, String.format("%020d", time)), true, new Key(metaRow).followingKey(PartialKey.ROW), false));
    int count = 0;
    
    String lastLog = null;
    
    loop1: for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifier().toString().equals("log")) {
        if (lastLog == null || !lastLog.equals(entry.getValue().toString()))
          System.out.println("Log : " + entry.getValue());
        lastLog = entry.getValue().toString();
      } else if (entry.getKey().getColumnQualifier().toString().equals("mut")) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(entry.getValue().get()));
        Mutation m = new Mutation();
        m.readFields(dis);
        
        LogFileValue lfv = new LogFileValue();
        lfv.mutations = Collections.singletonList(m);
        
        System.out.println(LogFileValue.format(lfv, 1));
        
        List<ColumnUpdate> columnsUpdates = m.getUpdates();
        for (ColumnUpdate cu : columnsUpdates) {
          if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(new Text(cu.getColumnFamily()), new Text(cu.getColumnQualifier())) && count > 0) {
            System.out.println("Saw change to prevrow, stopping printing events.");
            break loop1;
          }
        }
        count++;
      }
    }
    
  }
}
