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
package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public abstract class TestCfCqSlice {

  private static final String TABLE_NAME = "TestColumnSliceFilter";
  private static final Collection<Range> INFINITY = Collections.singletonList(new Range());
  private static final Lexicoder<Long> LONG_LEX = new ReadableLongLexicoder(4);
  private static final AtomicLong ROW_ID_GEN = new AtomicLong();

  private static final boolean easyThereSparky = false;
  private static final int LR_DIM = easyThereSparky ? 5 : 50;

  private static MiniAccumuloCluster mac;

  protected abstract Class getFilterClass();

  @BeforeClass
  public static void setupMAC() throws Exception {
    Path macPath = Files.createTempDirectory("mac");
    System.out.println("MAC running at " + macPath);
    MiniAccumuloConfig macCfg = new MiniAccumuloConfig(macPath.toFile(), "password");
    macCfg.setNumTservers(easyThereSparky ? 1 : 2);
    mac = new MiniAccumuloCluster(macCfg);
    mac.start();
    Collection<Mutation> largeRows = createMutations(LR_DIM, LR_DIM, LR_DIM);
    Connector conn = newConnector();
    conn.tableOperations().create(TABLE_NAME);
    if (!easyThereSparky) {
      SortedSet<Text> largeRowSplits = getSplits(0, LR_DIM - 1, 5);
      conn.tableOperations().addSplits(TABLE_NAME, largeRowSplits);
    }
    BatchWriter bw = conn.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
    bw.addMutations(largeRows);
    bw.flush();
  }

  private static Connector newConnector() throws AccumuloException, AccumuloSecurityException {
    return mac.getConnector("root", "password");
  }

  @AfterClass
  public static void tearDownMAC() throws Exception {
    mac.stop();
  }

  @Test
  public void testAllRowsFullSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class));
    bs.setRanges(INFINITY);
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
        }
      }
    }
  }

  @Test
  public void testSingleRowFullSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class));
    int rowId = LR_DIM / 2;
    bs.setRanges(Collections.singletonList(Range.exact(new Text(LONG_LEX.encode((long) rowId)))));
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (rowId == i) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  @Test
  public void testAllRowsSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 20;
    long sliceMinCq = 30;
    long sliceMaxCf = 25;
    long sliceMaxCq = 35;
    assertTrue("slice param must be less than LR_DIM", sliceMinCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMinCq < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCq < LR_DIM);
    Map<String,String> opts = new HashMap<String,String>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class, opts));
    bs.setRanges(INFINITY);
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j >= sliceMinCf && j <= sliceMaxCf && k >= sliceMinCq && k <= sliceMaxCq) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  @Test
  public void testSingleColumnSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 20;
    long sliceMinCq = 20;
    long sliceMaxCf = 20;
    long sliceMaxCq = 20;
    Map<String,String> opts = new HashMap<String,String>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class, opts));
    bs.setRanges(INFINITY);
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j == sliceMinCf && k == sliceMinCq) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  @Test
  public void testSingleColumnSliceByExclude() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 20;
    long sliceMinCq = 20;
    long sliceMaxCf = 22;
    long sliceMaxCq = 22;
    Map<String,String> opts = new HashMap<String,String>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class, opts));
    bs.setRanges(INFINITY);
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j == 21 && k == 21) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  private void loadKvs(boolean[][][] foundKvs, BatchScanner bs) {
    try {
      for (Map.Entry<Key,Value> kvPair : bs) {
        Key k = kvPair.getKey();
        int row = LONG_LEX.decode(k.getRow().copyBytes()).intValue();
        int cf = LONG_LEX.decode(k.getColumnFamily().copyBytes()).intValue();
        int cq = LONG_LEX.decode(k.getColumnQualifier().copyBytes()).intValue();
        foundKvs[row][cf][cq] = true;
      }
    } finally {
      bs.close();
    }
  }

  @Test
  public void testEmptySlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    // TODO: test with a batch scanner
    BatchScanner bs = newConnector().createBatchScanner(TABLE_NAME, new Authorizations(), 5);
    long sliceMinCf = LR_DIM + 1;
    long sliceMinCq = LR_DIM + 1;
    long sliceMaxCf = LR_DIM + 1;
    long sliceMaxCq = LR_DIM + 1;
    Map<String,String> opts = new HashMap<String,String>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    bs.addScanIterator(new IteratorSetting(50, "ColumnSliceFilter", ColumnSliceFilter.class, opts));
    bs.setRanges(INFINITY);
    loadKvs(foundKvs, bs);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
        }
      }
    }
  }

  /**
   * Rows 0..(LR_DIM - 1) will each have LR_DIM CFs, each with LR_DIM CQs
   *
   * For instance if LR_DIM is 3, (cf,cq) r: val
   *
   * (0,0) (0,1) (0,2) (1,0) (1,1) (1,2) (2,0) (2,1) (2,2) 0 0 1 2 3 4 5 6 7 8 1 9 10 11 12 13 14 15 16 17 2 18 19 20 21 22 23 24 25 26
   */
  static Collection<Mutation> createMutations(int numRows, int numCfs, int numCqs) {
    List<Mutation> muts = new LinkedList<Mutation>();
    for (int i = 0; i < numRows; i++) {
      byte[] rowId = LONG_LEX.encode(ROW_ID_GEN.getAndIncrement());
      Mutation newRow = new Mutation(rowId);
      muts.add(newRow);
      for (int j = 0; j < numCfs; j++) {
        for (int k = 0; k < numCqs; k++) {
          byte[] cf = LONG_LEX.encode((long) j);
          byte[] cq = LONG_LEX.encode((long) k);
          byte[] val = LONG_LEX.encode((long) (i * numCfs + j * numCqs + k));
          newRow.put(cf, cq, val);
        }
      }
    }
    return muts;
  }

  static SortedSet<Text> getSplits(int firstRow, int lastRow, int numParts) {
    SortedSet<Text> splits = new TreeSet<Text>();
    int numRows = (lastRow - firstRow) + 1;
    int rowsPerPart = numRows / numParts;
    for (long i = 1; i < numParts; i++) {
      splits.add(new Text(LONG_LEX.encode(rowsPerPart * i)));
    }
    return splits;
  }

  static class ReadableLongLexicoder implements Lexicoder<Long> {
    final String fmtStr;

    public ReadableLongLexicoder() {
      this(20);
    }

    public ReadableLongLexicoder(int numDigits) {
      fmtStr = "%0" + numDigits + "d";
    }

    @Override
    public byte[] encode(Long l) {
      return String.format(fmtStr, l).getBytes(UTF_8);
    }

    @Override
    public Long decode(byte[] b) throws ValueFormatException {
      return Long.parseLong(new String(b, UTF_8));
    }
  }
}
