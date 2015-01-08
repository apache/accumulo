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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.trace.instrument.CountSampler;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class ContinuousIngest {

  static public class BaseOpts extends ClientOnDefaultTable {
    public class DebugConverter implements IStringConverter<String> {
      @Override
      public String convert(String debugLog) {
        Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
        logger.setLevel(Level.TRACE);
        logger.setAdditivity(false);
        try {
          logger.addAppender(new FileAppender(new PatternLayout("%d{dd HH:mm:ss,SSS} [%-8c{2}] %-5p: %m%n"), debugLog, true));
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
        return debugLog;
      }
    }

    @Parameter(names = "--min", description = "lowest random row number to use")
    long min = 0;

    @Parameter(names = "--max", description = "maximum random row number to use")
    long max = Long.MAX_VALUE;

    @Parameter(names = "--debugLog", description = "file to write debugging output", converter = DebugConverter.class)
    String debugLog = null;

    BaseOpts() {
      super("ci");
    }
  }

  public static class ShortConverter implements IStringConverter<Short> {
    @Override
    public Short convert(String value) {
      return Short.valueOf(value);
    }
  }

  static public class Opts extends BaseOpts {
    @Parameter(names = "--num", description = "the number of entries to ingest")
    long num = Long.MAX_VALUE;

    @Parameter(names = "--maxColF", description = "maximum column family value to use", converter = ShortConverter.class)
    short maxColF = Short.MAX_VALUE;

    @Parameter(names = "--maxColQ", description = "maximum column qualifier value to use", converter = ShortConverter.class)
    short maxColQ = Short.MAX_VALUE;

    @Parameter(names = "--addCheckSum", description = "turn on checksums")
    boolean checksum = false;

    @Parameter(names = "--visibilities", description = "read the visibilities to ingest with from a file")
    String visFile = null;
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static List<ColumnVisibility> visibilities;

  private static void initVisibilities(Opts opts) throws Exception {
    if (opts.visFile == null) {
      visibilities = Collections.singletonList(new ColumnVisibility());
      return;
    }

    visibilities = new ArrayList<ColumnVisibility>();

    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(opts.visFile)), UTF_8));

    String line;

    while ((line = in.readLine()) != null) {
      visibilities.add(new ColumnVisibility(line));
    }

    in.close();
  }

  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }

  public static void main(String[] args) throws Exception {

    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(ContinuousIngest.class.getName(), args, bwOpts);

    initVisibilities(opts);

    if (opts.min < 0 || opts.max < 0 || opts.max <= opts.min) {
      throw new IllegalArgumentException("bad min and max");
    }
    Connector conn = opts.getConnector();

    if (!conn.tableOperations().exists(opts.getTableName())) {
      throw new TableNotFoundException(null, opts.getTableName(), "Consult the README and create the table before starting ingest.");
    }

    BatchWriter bw = conn.createBatchWriter(opts.getTableName(), bwOpts.getBatchWriterConfig());
    bw = Trace.wrapAll(bw, new CountSampler(1024));

    Random r = new Random();

    byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(UTF_8);

    System.out.printf("UUID %d %s%n", System.currentTimeMillis(), new String(ingestInstanceId, UTF_8));

    long count = 0;
    final int flushInterval = 1000000;
    final int maxDepth = 25;

    // always want to point back to flushed data. This way the previous item should
    // always exist in accumulo when verifying data. To do this make insert N point
    // back to the row from insert (N - flushInterval). The array below is used to keep
    // track of this.
    long prevRows[] = new long[flushInterval];
    long firstRows[] = new long[flushInterval];
    int firstColFams[] = new int[flushInterval];
    int firstColQuals[] = new int[flushInterval];

    long lastFlushTime = System.currentTimeMillis();

    out: while (true) {
      // generate first set of nodes
      ColumnVisibility cv = getVisibility(r);

      for (int index = 0; index < flushInterval; index++) {
        long rowLong = genLong(opts.min, opts.max, r);
        prevRows[index] = rowLong;
        firstRows[index] = rowLong;

        int cf = r.nextInt(opts.maxColF);
        int cq = r.nextInt(opts.maxColQ);

        firstColFams[index] = cf;
        firstColQuals[index] = cq;

        Mutation m = genMutation(rowLong, cf, cq, cv, ingestInstanceId, count, null, r, opts.checksum);
        count++;
        bw.addMutation(m);
      }

      lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
      if (count >= opts.num)
        break out;

      // generate subsequent sets of nodes that link to previous set of nodes
      for (int depth = 1; depth < maxDepth; depth++) {
        for (int index = 0; index < flushInterval; index++) {
          long rowLong = genLong(opts.min, opts.max, r);
          byte[] prevRow = genRow(prevRows[index]);
          prevRows[index] = rowLong;
          Mutation m = genMutation(rowLong, r.nextInt(opts.maxColF), r.nextInt(opts.maxColQ), cv, ingestInstanceId, count, prevRow, r, opts.checksum);
          count++;
          bw.addMutation(m);
        }

        lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
        if (count >= opts.num)
          break out;
      }

      // create one big linked list, this makes all of the first inserts
      // point to something
      for (int index = 0; index < flushInterval - 1; index++) {
        Mutation m = genMutation(firstRows[index], firstColFams[index], firstColQuals[index], cv, ingestInstanceId, count, genRow(prevRows[index + 1]), r,
            opts.checksum);
        count++;
        bw.addMutation(m);
      }
      lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
      if (count >= opts.num)
        break out;
    }

    bw.close();
    opts.stopTracing();
  }

  private static long flush(BatchWriter bw, long count, final int flushInterval, long lastFlushTime) throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    System.out.printf("FLUSH %d %d %d %d %d%n", t2, (t2 - lastFlushTime), (t2 - t1), count, flushInterval);
    lastFlushTime = t2;
    return lastFlushTime;
  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv, byte[] ingestInstanceId, long count, byte[] prevRow, Random r,
      boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not good for small data.... so used CRC32 instead
    CRC32 cksum = null;

    byte[] rowString = genRow(rowLong);

    byte[] cfString = FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    byte[] cqString = FastFormat.toZeroPaddedString(cqInt, 4, 16, EMPTY_BYTES);

    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }

    Mutation m = new Mutation(new Text(rowString));

    m.put(new Text(cfString), new Text(cqString), cv, createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }

  public static final long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffl) % (max - min)) + min;
  }

  static final byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }

  static final byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }

  private static Value createValue(byte[] ingestInstanceId, long count, byte[] prevRow, Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte val[] = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, count, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;
    val[index++] = ':';
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }

    val[index++] = ':';

    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }

    // System.out.println("val "+new String(val));

    return new Value(val);
  }
}
