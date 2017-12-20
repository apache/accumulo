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
package org.apache.accumulo.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Trace;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class VerifyIngest {

  private static final Logger log = LoggerFactory.getLogger(VerifyIngest.class);

  public static int getRow(Key k) {
    return Integer.parseInt(k.getRow().toString().split("_")[1]);
  }

  public static int getCol(Key k) {
    return Integer.parseInt(k.getColumnQualifier().toString().split("_")[1]);
  }

  public static class Opts extends TestIngest.Opts {
    @Parameter(names = "-useGet", description = "fetches values one at a time, instead of scanning")
    public boolean useGet = false;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(VerifyIngest.class.getName(), args, scanOpts);
    try {
      if (opts.trace) {
        String name = VerifyIngest.class.getSimpleName();
        DistributedTrace.enable();
        Trace.on(name);
        Trace.data("cmdLine", Arrays.asList(args).toString());
      }

      verifyIngest(opts.getConnector(), opts, scanOpts);

    } finally {
      Trace.off();
      DistributedTrace.disable();
    }
  }

  public static void verifyIngest(Connector connector, Opts opts, ScannerOpts scanOpts) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    byte[][] bytevals = TestIngest.generateValues(opts.dataSize);

    Authorizations labelAuths = new Authorizations("L1", "L2", "G1", "GROUP2");
    connector.securityOperations().changeUserAuthorizations(opts.getPrincipal(), labelAuths);

    int expectedRow = opts.startRow;
    int expectedCol = 0;
    int recsRead = 0;

    long bytesRead = 0;
    long t1 = System.currentTimeMillis();

    byte randomValue[] = new byte[opts.dataSize];
    Random random = new Random();

    Key endKey = new Key(new Text("row_" + String.format("%010d", opts.rows + opts.startRow)));

    int errors = 0;

    while (expectedRow < (opts.rows + opts.startRow)) {

      if (opts.useGet) {
        Text rowKey = new Text("row_" + String.format("%010d", expectedRow + opts.startRow));
        Text colf = new Text(opts.columnFamily);
        Text colq = new Text("col_" + String.format("%07d", expectedCol));

        try (Scanner scanner = connector.createScanner("test_ingest", labelAuths)) {
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
          if (opts.random != null) {
            ev = TestIngest.genRandomValue(random, randomValue, opts.random.intValue(), expectedRow, expectedCol);
          } else {
            ev = bytevals[expectedCol % bytevals.length];
          }

          if (val == null) {
            log.error("Did not find {} {} {}", rowKey, colf, colq);
            errors++;
          } else {
            recsRead++;
            bytesRead += val.length;
            Value value = new Value(val);
            if (value.compareTo(ev) != 0) {
              log.error("unexpected value  ({} {} {} : saw {} expected {}", rowKey, colf, colq, value, new Value(ev));
              errors++;
            }
          }

          expectedCol++;
          if (expectedCol >= opts.cols) {
            expectedCol = 0;
            expectedRow++;
          }
        }
      } else {

        Key startKey = new Key(new Text("row_" + String.format("%010d", expectedRow)));

        try (Scanner scanner = connector.createScanner(opts.getTableName(), labelAuths)) {
          scanner.setBatchSize(scanOpts.scanBatchSize);
          scanner.setRange(new Range(startKey, endKey));
          for (int j = 0; j < opts.cols; j++) {
            scanner.fetchColumn(new Text(opts.columnFamily), new Text("col_" + String.format("%07d", j)));
          }

          int recsReadBefore = recsRead;

          for (Entry<Key,Value> entry : scanner) {

            recsRead++;

            bytesRead += entry.getKey().getLength();
            bytesRead += entry.getValue().getSize();

            int rowNum = getRow(entry.getKey());
            int colNum = getCol(entry.getKey());

            if (rowNum != expectedRow) {
              log.error("rowNum != expectedRow   {} != {}", rowNum, expectedRow);
              errors++;
              expectedRow = rowNum;
            }

            if (colNum != expectedCol) {
              log.error("colNum != expectedCol  {} != {}  rowNum : {}", colNum, expectedCol, rowNum);
              errors++;
            }

            if (expectedRow >= (opts.rows + opts.startRow)) {
              log.error("expectedRow ({}) >= (ingestArgs.rows + ingestArgs.startRow)  ({}), get batch returned data passed end key", expectedRow,
                  (opts.rows + opts.startRow));
              errors++;
              break;
            }

            byte value[];
            if (opts.random != null) {
              value = TestIngest.genRandomValue(random, randomValue, opts.random.intValue(), expectedRow, colNum);
            } else {
              value = bytevals[colNum % bytevals.length];
            }

            if (entry.getValue().compareTo(value) != 0) {
              log.error("unexpected value, rowNum : {} colNum : {}", rowNum, colNum);
              log.error(" saw = {} expected = {}", new String(entry.getValue().get()), new String(value));
              errors++;
            }

            if (opts.timestamp >= 0 && entry.getKey().getTimestamp() != opts.timestamp) {
              log.error("unexpected timestamp {}, rowNum : {} colNum : {}", entry.getKey().getTimestamp(), rowNum, colNum);
              errors++;
            }

            expectedCol++;
            if (expectedCol >= opts.cols) {
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
    }

    long t2 = System.currentTimeMillis();

    if (errors > 0) {
      throw new AccumuloException("saw " + errors + " errors ");
    }

    if (expectedRow != (opts.rows + opts.startRow)) {
      throw new AccumuloException("Did not read expected number of rows. Saw " + (expectedRow - opts.startRow) + " expected " + opts.rows);
    } else {
      System.out.printf("%,12d records read | %,8d records/sec | %,12d bytes read | %,8d bytes/sec | %6.3f secs   %n", recsRead,
          (int) ((recsRead) / ((t2 - t1) / 1000.0)), bytesRead, (int) (bytesRead / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0);
    }
  }

}
