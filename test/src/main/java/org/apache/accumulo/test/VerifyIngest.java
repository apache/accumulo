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
package org.apache.accumulo.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class VerifyIngest {

  private static final Logger log = LoggerFactory.getLogger(VerifyIngest.class);

  public static int getRow(Key k) {
    return Integer.parseInt(k.getRow().toString().split("_")[1]);
  }

  public static int getCol(Key k) {
    return Integer.parseInt(k.getColumnQualifier().toString().split("_")[1]);
  }

  public static class VerifyParams extends TestIngest.IngestParams {
    public boolean useGet = false;

    public VerifyParams(Properties props) {
      super(props);
    }

    public VerifyParams(Properties props, String table) {
      super(props, table);
    }

    public VerifyParams(Properties props, String table, int rows) {
      super(props, table, rows);
    }
  }

  public static class Opts extends TestIngest.Opts {
    @Parameter(names = "-useGet", description = "fetches values one at a time, instead of scanning")
    public boolean useGet = false;

    public VerifyParams getVerifyParams() {
      VerifyParams params = new VerifyParams(getClientProps(), tableName);
      populateIngestPrams(params);
      params.useGet = useGet;
      return params;
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(VerifyIngest.class.getName(), args);
    Span span = TraceUtil.startSpan(VerifyIngest.class, "main");
    try (Scope scope = span.makeCurrent()) {

      span.setAttribute("cmdLine", Arrays.asList(args).toString());

      try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
        verifyIngest(client, opts.getVerifyParams());
      }

    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  public static void verifyIngest(AccumuloClient accumuloClient, VerifyParams params)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    byte[][] bytevals = TestIngest.generateValues(params.dataSize);

    Authorizations labelAuths = new Authorizations("L1", "L2", "G1", "GROUP2");
    String principal = ClientProperty.AUTH_PRINCIPAL.getValue(params.clientProps);
    accumuloClient.securityOperations().changeUserAuthorizations(principal, labelAuths);

    int expectedRow = params.startRow;
    int expectedCol = 0;
    int recsRead = 0;

    long bytesRead = 0;
    long t1 = System.currentTimeMillis();

    byte[] randomValue = new byte[params.dataSize];

    Key endKey = new Key(new Text("row_" + String.format("%010d", params.rows + params.startRow)));

    int errors = 0;

    while (expectedRow < (params.rows + params.startRow)) {

      if (params.useGet) {
        Text rowKey = new Text("row_" + String.format("%010d", expectedRow + params.startRow));
        Text colf = new Text(params.columnFamily);
        Text colq = new Text("col_" + String.format("%07d", expectedCol));

        try (Scanner scanner = accumuloClient.createScanner("test_ingest", labelAuths)) {
          scanner.setBatchSize(1);
          Key startKey = new Key(rowKey, colf, colq);
          Range range = new Range(startKey, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL));
          scanner.setRange(range);

          byte[] val = null; // t.get(rowKey, column);

          Iterator<Entry<Key,Value>> iter = scanner.iterator();

          if (iter.hasNext()) {
            val = iter.next().getValue().get();
          }

          byte[] ev;
          if (params.random != null) {
            ev = TestIngest.genRandomValue(randomValue, params.random, expectedRow, expectedCol);
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
              log.error("unexpected value  ({} {} {} : saw {} expected {}", rowKey, colf, colq,
                  value, new Value(ev));
              errors++;
            }
          }

          expectedCol++;
          if (expectedCol >= params.cols) {
            expectedCol = 0;
            expectedRow++;
          }
        }
      } else {

        Key startKey = new Key(new Text("row_" + String.format("%010d", expectedRow)));

        try (Scanner scanner = accumuloClient.createScanner(params.tableName, labelAuths)) {
          scanner.setRange(new Range(startKey, endKey));
          for (int j = 0; j < params.cols; j++) {
            scanner.fetchColumn(new Text(params.columnFamily),
                new Text("col_" + String.format("%07d", j)));
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
              log.error("colNum != expectedCol  {} != {}  rowNum : {}", colNum, expectedCol,
                  rowNum);
              errors++;
            }

            if (expectedRow >= (params.rows + params.startRow)) {
              log.error(
                  "expectedRow ({}) >= (ingestArgs.rows + ingestArgs.startRow)  ({}), get"
                      + " batch returned data passed end key",
                  expectedRow, (params.rows + params.startRow));
              errors++;
              break;
            }

            byte[] value;
            if (params.random != null) {
              value = TestIngest.genRandomValue(randomValue, params.random, expectedRow, colNum);
            } else {
              value = bytevals[colNum % bytevals.length];
            }

            if (entry.getValue().compareTo(value) != 0) {
              log.error("unexpected value, rowNum : {} colNum : {}", rowNum, colNum);
              log.error(" saw = {} expected = {}", new String(entry.getValue().get()),
                  new String(value));
              errors++;
            }

            if (params.timestamp >= 0 && entry.getKey().getTimestamp() != params.timestamp) {
              log.error("unexpected timestamp {}, rowNum : {} colNum : {}",
                  entry.getKey().getTimestamp(), rowNum, colNum);
              errors++;
            }

            expectedCol++;
            if (expectedCol >= params.cols) {
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

    if (expectedRow == (params.rows + params.startRow)) {
      System.out.printf(
          "%,12d records read | %,8d records/sec | %,12d bytes read |"
              + " %,8d bytes/sec | %6.3f secs   %n",
          recsRead, (int) (recsRead / ((t2 - t1) / 1000.0)), bytesRead,
          (int) (bytesRead / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0);
    } else {
      throw new AccumuloException("Did not read expected number of rows. Saw "
          + (expectedRow - params.startRow) + " expected " + params.rows);
    }
  }

}
