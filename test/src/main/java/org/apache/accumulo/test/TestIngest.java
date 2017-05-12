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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.TabletServerBatchWriter;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class TestIngest {
  public static final Authorizations AUTHS = new Authorizations("L1", "L2", "G1", "GROUP2");

  public static class Opts extends ClientOnDefaultTable {

    @Parameter(names = "--createTable")
    public boolean createTable = false;

    @Parameter(names = "--splits", description = "the number of splits to use when creating the table")
    public int numsplits = 1;

    @Parameter(names = "--start", description = "the starting row number")
    public int startRow = 0;

    @Parameter(names = "--rows", description = "the number of rows to ingest")
    public int rows = 100000;

    @Parameter(names = "--cols", description = "the number of columns to ingest per row")
    public int cols = 1;

    @Parameter(names = "--random", description = "insert random rows and use the given number to seed the psuedo-random number generator")
    public Integer random = null;

    @Parameter(names = "--size", description = "the size of the value to ingest")
    public int dataSize = 1000;

    @Parameter(names = "--delete", description = "delete values instead of inserting them")
    public boolean delete = false;

    @Parameter(names = {"-ts", "--timestamp"}, description = "timestamp to use for all values")
    public long timestamp = -1;

    @Parameter(names = "--rfile", description = "generate data into a file that can be imported")
    public String outputFile = null;

    @Parameter(names = "--stride", description = "the difference between successive row ids")
    public int stride;

    @Parameter(names = {"-cf", "--columnFamily"}, description = "place columns in this column family")
    public String columnFamily = "colf";

    @Parameter(names = {"-cv", "--columnVisibility"}, description = "place columns in this column family", converter = VisibilityConverter.class)
    public ColumnVisibility columnVisibility = new ColumnVisibility();

    public Configuration conf = null;
    public FileSystem fs = null;

    public Opts() {
      super("test_ingest");
    }
  }

  public static void createTable(Connector conn, Opts args) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    if (args.createTable) {
      TreeSet<Text> splits = getSplitPoints(args.startRow, args.startRow + args.rows, args.numsplits);

      if (!conn.tableOperations().exists(args.getTableName()))
        conn.tableOperations().create(args.getTableName());
      try {
        conn.tableOperations().addSplits(args.getTableName(), splits);
      } catch (TableNotFoundException ex) {
        // unlikely
        throw new RuntimeException(ex);
      }
    }
  }

  public static TreeSet<Text> getSplitPoints(long start, long end, long numsplits) {
    long splitSize = (end - start) / numsplits;

    long pos = start + splitSize;

    TreeSet<Text> splits = new TreeSet<>();

    while (pos < end) {
      splits.add(new Text(String.format("row_%010d", pos)));
      pos += splitSize;
    }
    return splits;
  }

  public static byte[][] generateValues(int dataSize) {

    byte[][] bytevals = new byte[10][];

    byte[] letters = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    for (int i = 0; i < 10; i++) {
      bytevals[i] = new byte[dataSize];
      for (int j = 0; j < dataSize; j++)
        bytevals[i][j] = letters[i];
    }
    return bytevals;
  }

  private static byte ROW_PREFIX[] = "row_".getBytes(UTF_8);
  private static byte COL_PREFIX[] = "col_".getBytes(UTF_8);

  public static Text generateRow(int rowid, int startRow) {
    return new Text(FastFormat.toZeroPaddedString(rowid + startRow, 10, 10, ROW_PREFIX));
  }

  public static byte[] genRandomValue(Random random, byte dest[], int seed, int row, int col) {
    random.setSeed((row ^ seed) ^ col);
    random.nextBytes(dest);
    toPrintableChars(dest);

    return dest;
  }

  public static void toPrintableChars(byte[] dest) {
    // transform to printable chars
    for (int i = 0; i < dest.length; i++) {
      dest[i] = (byte) (((0xff & dest[i]) % 92) + ' ');
    }
  }

  public static void main(String[] args) throws Exception {

    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(TestIngest.class.getName(), args, bwOpts);

    String name = TestIngest.class.getSimpleName();
    DistributedTrace.enable(name);

    try {
      opts.startTracing(name);

      if (opts.debug)
        Logger.getLogger(TabletServerBatchWriter.class.getName()).setLevel(Level.TRACE);

      // test batch update

      ingest(opts.getConnector(), opts, bwOpts);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      opts.stopTracing();
      DistributedTrace.disable();
    }
  }

  public static void ingest(Connector connector, FileSystem fs, Opts opts, BatchWriterOpts bwOpts) throws IOException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException, MutationsRejectedException, TableExistsException {
    long stopTime;

    byte[][] bytevals = generateValues(opts.dataSize);

    byte randomValue[] = new byte[opts.dataSize];
    Random random = new Random();

    long bytesWritten = 0;

    createTable(connector, opts);

    BatchWriter bw = null;
    FileSKVWriter writer = null;

    if (opts.outputFile != null) {
      Configuration conf = CachedConfiguration.getInstance();
      writer = FileOperations.getInstance().newWriterBuilder().forFile(opts.outputFile + "." + RFile.EXTENSION, fs, conf)
          .withTableConfiguration(DefaultConfiguration.getInstance()).build();
      writer.startDefaultLocalityGroup();
    } else {
      bw = connector.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
      connector.securityOperations().changeUserAuthorizations(opts.getPrincipal(), AUTHS);
    }
    Text labBA = new Text(opts.columnVisibility.getExpression());

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < opts.rows; i++) {
      int rowid;
      if (opts.stride > 0) {
        rowid = ((i % opts.stride) * (opts.rows / opts.stride)) + (i / opts.stride);
      } else {
        rowid = i;
      }

      Text row = generateRow(rowid, opts.startRow);
      Mutation m = new Mutation(row);
      for (int j = 0; j < opts.cols; j++) {
        Text colf = new Text(opts.columnFamily);
        Text colq = new Text(FastFormat.toZeroPaddedString(j, 7, 10, COL_PREFIX));

        if (writer != null) {
          Key key = new Key(row, colf, colq, labBA);
          if (opts.timestamp >= 0) {
            key.setTimestamp(opts.timestamp);
          } else {
            key.setTimestamp(startTime);
          }

          if (opts.delete) {
            key.setDeleted(true);
          } else {
            key.setDeleted(false);
          }

          bytesWritten += key.getSize();

          if (opts.delete) {
            writer.append(key, new Value(new byte[0]));
          } else {
            byte value[];
            if (opts.random != null) {
              value = genRandomValue(random, randomValue, opts.random.intValue(), rowid + opts.startRow, j);
            } else {
              value = bytevals[j % bytevals.length];
            }

            Value v = new Value(value);
            writer.append(key, v);
            bytesWritten += v.getSize();
          }

        } else {
          Key key = new Key(row, colf, colq, labBA);
          bytesWritten += key.getSize();

          if (opts.delete) {
            if (opts.timestamp >= 0)
              m.putDelete(colf, colq, opts.columnVisibility, opts.timestamp);
            else
              m.putDelete(colf, colq, opts.columnVisibility);
          } else {
            byte value[];
            if (opts.random != null) {
              value = genRandomValue(random, randomValue, opts.random.intValue(), rowid + opts.startRow, j);
            } else {
              value = bytevals[j % bytevals.length];
            }
            bytesWritten += value.length;

            if (opts.timestamp >= 0) {
              m.put(colf, colq, opts.columnVisibility, opts.timestamp, new Value(value, true));
            } else {
              m.put(colf, colq, opts.columnVisibility, new Value(value, true));

            }
          }
        }

      }
      if (bw != null)
        bw.addMutation(m);

    }

    if (writer != null) {
      writer.close();
    } else if (bw != null) {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        if (e.getSecurityErrorCodes().size() > 0) {
          for (Entry<TabletId,Set<SecurityErrorCode>> entry : e.getSecurityErrorCodes().entrySet()) {
            System.err.println("ERROR : Not authorized to write to : " + entry.getKey() + " due to " + entry.getValue());
          }
        }

        if (e.getConstraintViolationSummaries().size() > 0) {
          for (ConstraintViolationSummary cvs : e.getConstraintViolationSummaries()) {
            System.err.println("ERROR : Constraint violates : " + cvs);
          }
        }

        throw e;
      }
    }

    stopTime = System.currentTimeMillis();

    int totalValues = opts.rows * opts.cols;
    double elapsed = (stopTime - startTime) / 1000.0;

    System.out.printf("%,12d records written | %,8d records/sec | %,12d bytes written | %,8d bytes/sec | %6.3f secs   %n", totalValues,
        (int) (totalValues / elapsed), bytesWritten, (int) (bytesWritten / elapsed), elapsed);
  }

  public static void ingest(Connector c, Opts opts, BatchWriterOpts batchWriterOpts) throws MutationsRejectedException, IOException, AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TableExistsException {
    ingest(c, FileSystem.get(CachedConfiguration.getInstance()), opts, batchWriterOpts);
  }
}
