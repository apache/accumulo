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
package org.apache.accumulo.server.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooReader;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.test.TestIngest.IngestArgs;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class VerifyIngest {
  private static String username = "root";
  private static byte[] passwd = "secret".getBytes();
  
  private static final Logger log = Logger.getLogger(VerifyIngest.class);
  
  public static int getRow(Key k) {
    return Integer.parseInt(k.getRow().toString().split("_")[1]);
  }
  
  public static int getCol(Key k) {
    return Integer.parseInt(k.getColumnQualifier().toString().split("_")[1]);
  }
  
  public static void main(String[] args) {
    IngestArgs ingestArgs = TestIngest.parseArgs(args);
    Instance instance = HdfsZooInstance.getInstance();
    try {
      if (ingestArgs.trace) {
        String name = VerifyIngest.class.getSimpleName();
        DistributedTrace.enable(instance, new ZooReader(instance), name, null);
        Trace.on(name);
        Trace.currentTrace().data("cmdLine", Arrays.asList(args).toString());
      }
      
      Connector connector = null;
      while (connector == null) {
        try {
          connector = instance.getConnector(username, passwd);
        } catch (AccumuloException e) {
          log.warn("Could not connect to accumulo; will retry: " + e);
          UtilWaitThread.sleep(1000);
        }
      }
      
      byte[][] bytevals = TestIngest.generateValues(ingestArgs);
      
      Authorizations labelAuths = new Authorizations("L1", "L2", "G1", "GROUP2");
      
      int expectedRow = ingestArgs.startRow;
      int expectedCol = 0;
      int recsRead = 0;
      
      long bytesRead = 0;
      long t1 = System.currentTimeMillis();
      
      byte randomValue[] = new byte[ingestArgs.dataSize];
      Random random = new Random();
      
      Key endKey = new Key(new Text("row_" + String.format("%010d", ingestArgs.rows + ingestArgs.startRow)));
      
      int errors = 0;
      
      while (expectedRow < (ingestArgs.rows + ingestArgs.startRow)) {
        
        if (ingestArgs.useGet) {
          Text rowKey = new Text("row_" + String.format("%010d", expectedRow + ingestArgs.startRow));
          Text colf = new Text(ingestArgs.columnFamily);
          Text colq = new Text("col_" + String.format("%05d", expectedCol));
          
          Scanner scanner = connector.createScanner("test_ingest", labelAuths);
          scanner.setBatchSize(1);
          Key startKey = new Key(rowKey, colf, colq);
          Range range = new Range(startKey, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL));
          scanner.setRange(range);
          
          byte[] val = null; // t.get(rowKey, column);
          
          Iterator<Entry<Key,Value>> iter = scanner.iterator();
          
          if (iter.hasNext()) {
            val = iter.next().getValue().get();
          }
          
          byte ev[];
          if (ingestArgs.random) {
            ev = TestIngest.genRandomValue(random, randomValue, ingestArgs.seed, expectedRow, expectedCol);
          } else {
            ev = bytevals[expectedCol % bytevals.length];
          }
          
          if (val == null) {
            log.error("Did not find " + rowKey + " " + colf + " " + colq);
            errors++;
          } else {
            recsRead++;
            bytesRead += val.length;
            Value value = new Value(val);
            if (value.compareTo(ev) != 0) {
              log.error("unexpected value  (" + rowKey + " " + colf + " " + colq + " : saw " + value + " expected " + new Value(ev));
              errors++;
            }
          }
          
          expectedCol++;
          if (expectedCol >= ingestArgs.cols) {
            expectedCol = 0;
            expectedRow++;
          }
          
        } else {
          
          int batchSize = 10000;
          
          Key startKey = new Key(new Text("row_" + String.format("%010d", expectedRow)));
          
          Scanner scanner = connector.createScanner("test_ingest", labelAuths);
          scanner.setBatchSize(batchSize);
          scanner.setRange(new Range(startKey, endKey));
          for (int j = 0; j < ingestArgs.cols; j++) {
            scanner.fetchColumn(new Text(ingestArgs.columnFamily), new Text("col_" + String.format("%05d", j)));
          }
          
          int recsReadBefore = recsRead;
          
          for (Entry<Key,Value> entry : scanner) {
            
            recsRead++;
            
            bytesRead += entry.getKey().getLength();
            bytesRead += entry.getValue().getSize();
            
            int rowNum = getRow(entry.getKey());
            int colNum = getCol(entry.getKey());
            
            if (rowNum != expectedRow) {
              log.error("rowNum != expectedRow   " + rowNum + " != " + expectedRow);
              errors++;
              expectedRow = rowNum;
            }
            
            if (colNum != expectedCol) {
              log.error("colNum != expectedCol  " + colNum + " != " + expectedCol + "  rowNum : " + rowNum);
              errors++;
            }
            
            if (expectedRow >= (ingestArgs.rows + ingestArgs.startRow)) {
              log.error("expectedRow (" + expectedRow + ") >= (ingestArgs.rows + ingestArgs.startRow)  (" + (ingestArgs.rows + ingestArgs.startRow)
                  + "), get batch returned data passed end key");
              errors++;
              break;
            }
            
            byte value[];
            if (ingestArgs.random) {
              value = TestIngest.genRandomValue(random, randomValue, ingestArgs.seed, expectedRow, colNum);
            } else {
              value = bytevals[colNum % bytevals.length];
            }
            
            if (entry.getValue().compareTo(value) != 0) {
              log.error("unexpected value, rowNum : " + rowNum + " colNum : " + colNum);
              log.error(" saw = " + new String(entry.getValue().get()) + " expected = " + new String(value));
              errors++;
            }
            
            if (ingestArgs.hasTimestamp && entry.getKey().getTimestamp() != ingestArgs.timestamp) {
              log.error("unexpected timestamp " + entry.getKey().getTimestamp() + ", rowNum : " + rowNum + " colNum : " + colNum);
              errors++;
            }
            
            expectedCol++;
            if (expectedCol >= ingestArgs.cols) {
              expectedCol = 0;
              expectedRow++;
            }
            
          }
          
          if (recsRead == recsReadBefore) {
            log.warn("Scan returned nothing, breaking...");
            break;
          }
          
        }
      }
      
      long t2 = System.currentTimeMillis();
      
      if (errors > 0) {
        log.error("saw " + errors + " errors ");
        System.exit(1);
      }
      
      if (expectedRow != (ingestArgs.rows + ingestArgs.startRow)) {
        log.error("Did not read expected number of rows. Saw " + (expectedRow - ingestArgs.startRow) + " expected " + ingestArgs.rows);
        System.exit(1);
      } else {
        System.out.printf("%,12d records read | %,8d records/sec | %,12d bytes read | %,8d bytes/sec | %6.3f secs   \n", recsRead,
            (int) ((recsRead) / ((t2 - t1) / 1000.0)), bytesRead, (int) (bytesRead / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0);
      }
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      Trace.off();
    }
  }
  
}
