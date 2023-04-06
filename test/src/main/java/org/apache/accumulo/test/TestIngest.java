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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
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
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TestIngest {
  public static final Authorizations AUTHS = new Authorizations("L1", "L2", "G1", "GROUP2");

  public static class IngestParams {
    public Properties clientProps = new Properties();
    public String tableName = "test_ingest";
    public boolean createTable = false;
    public int numsplits = 1;
    public int startRow = 0;
    public int rows = 100000;
    public int cols = 1;
    public Integer random = null;
    public int dataSize = 1000;
    public boolean delete = false;
    public long timestamp = -1;
    public String outputFile = null;
    public int stride;
    public String columnFamily = "colf";
    public ColumnVisibility columnVisibility = new ColumnVisibility();

    public IngestParams(Properties props) {
      clientProps = props;
    }

    public IngestParams(Properties props, String table) {
      this(props);
      tableName = table;
    }

    public IngestParams(Properties props, String table, int rows) {
      this(props, table);
      this.rows = rows;
    }
  }

  public static class Opts extends ClientOpts {
    @Parameter(names = "--table", description = "table to use")
    String tableName = "test_ingest";

    @Parameter(names = "--createTable")
    boolean createTable = false;

    @Parameter(names = "--splits",
        description = "the number of splits to use when creating the table")
    int numsplits = 1;

    @Parameter(names = "--start", description = "the starting row number")
    int startRow = 0;

    @Parameter(names = "--rows", description = "the number of rows to ingest")
    int rows = 100000;

    @Parameter(names = "--cols", description = "the number of columns to ingest per row")
    int cols = 1;

    @Parameter(names = "--random", description = "insert random rows and use"
        + " the given number to seed the pseudo-random number generator")
    Integer random = null;

    @Parameter(names = "--size", description = "the size of the value to ingest")
    int dataSize = 1000;

    @Parameter(names = "--delete", description = "delete values instead of inserting them")
    boolean delete = false;

    @Parameter(names = {"-ts", "--timestamp"}, description = "timestamp to use for all values")
    long timestamp = -1;

    @Parameter(names = "--rfile", description = "generate data into a file that can be imported")
    String outputFile = null;

    @Parameter(names = "--stride", description = "the difference between successive row ids")
    int stride;

    @Parameter(names = {"-cf", "--columnFamily"},
        description = "place columns in this column family")
    String columnFamily = "colf";

    @Parameter(names = {"-cv", "--columnVisibility"},
        description = "place columns in this column family", converter = VisibilityConverter.class)
    ColumnVisibility columnVisibility = new ColumnVisibility();

    protected void populateIngestPrams(IngestParams params) {
      params.createTable = createTable;
      params.numsplits = numsplits;
      params.startRow = startRow;
      params.rows = rows;
      params.cols = cols;
      params.random = random;
      params.dataSize = dataSize;
      params.delete = delete;
      params.timestamp = timestamp;
      params.outputFile = outputFile;
      params.stride = stride;
      params.columnFamily = columnFamily;
      params.columnVisibility = columnVisibility;
    }

    public IngestParams getIngestPrams() {
      IngestParams params = new IngestParams(getClientProps(), tableName);
      populateIngestPrams(params);
      return params;
    }
  }

  public static void createTable(AccumuloClient client, IngestParams params)
      throws AccumuloException, AccumuloSecurityException, TableExistsException {
    if (params.createTable) {
      TreeSet<Text> splits =
          getSplitPoints(params.startRow, params.startRow + params.rows, params.numsplits);
      // if the table does not exist, create it (with splits)
      if (!client.tableOperations().exists(params.tableName)) {
        NewTableConfiguration ntc = new NewTableConfiguration();
        if (!splits.isEmpty()) {
          ntc = ntc.withSplits(splits);
        }
        client.tableOperations().create(params.tableName, ntc);
      } else { // if the table already exists, add splits to it
        try {
          client.tableOperations().addSplits(params.tableName, splits);
        } catch (TableNotFoundException ex) {
          // unlikely
          throw new RuntimeException(ex);
        }
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
      for (int j = 0; j < dataSize; j++) {
        bytevals[i][j] = letters[i];
      }
    }
    return bytevals;
  }

  private static byte[] ROW_PREFIX = "row_".getBytes(UTF_8);
  private static byte[] COL_PREFIX = "col_".getBytes(UTF_8);

  public static Text generateRow(int rowid, int startRow) {
    return new Text(FastFormat.toZeroPaddedString(rowid + startRow, 10, 10, ROW_PREFIX));
  }

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random with specific seed is intended for this test")
  public static byte[] genRandomValue(byte[] dest, int seed, int row, int col) {
    var random = new Random((row ^ seed) ^ col);
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
    opts.parseArgs(TestIngest.class.getSimpleName(), args);

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      ingest(client, opts.getIngestPrams());
    }
  }

  public static void ingest(AccumuloClient accumuloClient, FileSystem fs, IngestParams params)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException,
      MutationsRejectedException, TableExistsException {
    long stopTime;

    byte[][] bytevals = generateValues(params.dataSize);

    byte[] randomValue = new byte[params.dataSize];

    long bytesWritten = 0;

    createTable(accumuloClient, params);

    BatchWriter bw = null;
    FileSKVWriter writer = null;

    if (params.outputFile != null) {
      ClientContext cc = (ClientContext) accumuloClient;
      writer = FileOperations.getInstance().newWriterBuilder()
          .forFile(params.outputFile + "." + RFile.EXTENSION, fs, cc.getHadoopConf(),
              NoCryptoServiceFactory.NONE)
          .withTableConfiguration(DefaultConfiguration.getInstance()).build();
      writer.startDefaultLocalityGroup();
    } else {
      bw = accumuloClient.createBatchWriter(params.tableName);
      String principal = ClientProperty.AUTH_PRINCIPAL.getValue(params.clientProps);
      accumuloClient.securityOperations().changeUserAuthorizations(principal, AUTHS);
    }
    Text labBA = new Text(params.columnVisibility.getExpression());

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < params.rows; i++) {
      int rowid;
      if (params.stride > 0) {
        rowid = ((i % params.stride) * (params.rows / params.stride)) + (i / params.stride);
      } else {
        rowid = i;
      }

      Text row = generateRow(rowid, params.startRow);
      Mutation m = new Mutation(row);
      for (int j = 0; j < params.cols; j++) {
        Text colf = new Text(params.columnFamily);
        Text colq = new Text(FastFormat.toZeroPaddedString(j, 7, 10, COL_PREFIX));

        if (writer != null) {
          Key key = new Key(row, colf, colq, labBA);
          if (params.timestamp >= 0) {
            key.setTimestamp(params.timestamp);
          } else {
            key.setTimestamp(startTime);
          }

          if (params.delete) {
            key.setDeleted(true);
          } else {
            key.setDeleted(false);
          }

          bytesWritten += key.getSize();

          if (params.delete) {
            writer.append(key, new Value());
          } else {
            byte[] value;
            if (params.random != null) {
              value = genRandomValue(randomValue, params.random, rowid + params.startRow, j);
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

          if (params.delete) {
            if (params.timestamp >= 0) {
              m.putDelete(colf, colq, params.columnVisibility, params.timestamp);
            } else {
              m.putDelete(colf, colq, params.columnVisibility);
            }
          } else {
            byte[] value;
            if (params.random != null) {
              value = genRandomValue(randomValue, params.random, rowid + params.startRow, j);
            } else {
              value = bytevals[j % bytevals.length];
            }
            bytesWritten += value.length;

            if (params.timestamp >= 0) {
              m.put(colf, colq, params.columnVisibility, params.timestamp, new Value(value, true));
            } else {
              m.put(colf, colq, params.columnVisibility, new Value(value, true));

            }
          }
        }

      }
      if (bw != null) {
        bw.addMutation(m);
      }
    }

    if (writer != null) {
      writer.close();
    } else if (bw != null) {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        if (!e.getSecurityErrorCodes().isEmpty()) {
          for (Entry<TabletId,Set<SecurityErrorCode>> entry : e.getSecurityErrorCodes()
              .entrySet()) {
            System.err.println("ERROR : Not authorized to write to : " + entry.getKey() + " due to "
                + entry.getValue());
          }
        }

        if (!e.getConstraintViolationSummaries().isEmpty()) {
          for (ConstraintViolationSummary cvs : e.getConstraintViolationSummaries()) {
            System.err.println("ERROR : Constraint violates : " + cvs);
          }
        }
        throw e;
      }
    }

    stopTime = System.currentTimeMillis();

    int totalValues = params.rows * params.cols;
    double elapsed = (stopTime - startTime) / 1000.0;

    System.out.printf(
        "%,12d records written | %,8d records/sec | %,12d bytes written"
            + " | %,8d bytes/sec | %6.3f secs   %n",
        totalValues, (int) (totalValues / elapsed), bytesWritten, (int) (bytesWritten / elapsed),
        elapsed);
  }

  public static void ingest(AccumuloClient c, IngestParams params)
      throws MutationsRejectedException, IOException, AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TableExistsException {
    ClientContext cc = (ClientContext) c;
    ingest(c, FileSystem.get(cc.getHadoopConf()), params);
  }
}
