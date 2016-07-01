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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestCfCqSlice {

  private static final Range INFINITY = new Range();
  private static final Lexicoder<Long> LONG_LEX = new ReadableLongLexicoder(4);
  private static final AtomicLong ROW_ID_GEN = new AtomicLong();

  private static final boolean easyThereSparky = false;
  private static final int LR_DIM = easyThereSparky ? 5 : 50;

  private static final Map<String,String> EMPTY_OPTS = Collections.emptyMap();
  private static final Set<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  protected abstract Class<? extends SortedKeyValueIterator<Key,Value>> getFilterClass();

  private static TreeMap<Key,Value> data;

  @BeforeClass
  public static void setupData() {
    data = createMap(LR_DIM, LR_DIM, LR_DIM);
  }

  @AfterClass
  public static void clearData() {
    data = null;
  }

  @Test
  public void testAllRowsFullSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    loadKvs(foundKvs, EMPTY_OPTS, INFINITY);
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
    int rowId = LR_DIM / 2;
    loadKvs(foundKvs, EMPTY_OPTS, Range.exact(new Text(LONG_LEX.encode((long) rowId))));
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
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    loadKvs(foundKvs, opts, INFINITY);
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
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    loadKvs(foundKvs, opts, INFINITY);
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
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    loadKvs(foundKvs, opts, INFINITY);
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

  @Test
  public void testAllCfsCqSlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCq = 10;
    long sliceMaxCq = 30;
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    loadKvs(foundKvs, opts, INFINITY);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (k >= sliceMinCq && k <= sliceMaxCq) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  @Test
  public void testSliceCfsAllCqs() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 10;
    long sliceMaxCf = 30;
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    loadKvs(foundKvs, opts, INFINITY);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j >= sliceMinCf && j <= sliceMaxCf) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  @Test
  public void testEmptySlice() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = LR_DIM + 1;
    long sliceMinCq = LR_DIM + 1;
    long sliceMaxCf = LR_DIM + 1;
    long sliceMaxCq = LR_DIM + 1;
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    loadKvs(foundKvs, opts, INFINITY);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
        }
      }
    }
  }

  @Test
  public void testStackedFilters() throws Exception {
    Map<String,String> firstOpts = new HashMap<>();
    Map<String,String> secondOpts = new HashMap<>();
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 20;
    long sliceMaxCf = 25;
    long sliceMinCq = 30;
    long sliceMaxCq = 35;
    assertTrue("slice param must be less than LR_DIM", sliceMinCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMinCq < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCq < LR_DIM);
    firstOpts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    firstOpts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    secondOpts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    secondOpts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    SortedKeyValueIterator<Key,Value> skvi = getFilterClass().newInstance();
    skvi.init(new SortedMapIterator(data), firstOpts, null);
    loadKvs(skvi.deepCopy(null), foundKvs, secondOpts, INFINITY);
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
  public void testSeekMinExclusive() throws Exception {
    boolean[][][] foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    long sliceMinCf = 20;
    long sliceMinCq = 30;
    long sliceMaxCf = 25;
    long sliceMaxCq = 35;
    assertTrue("slice param must be less than LR_DIM", sliceMinCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMinCq < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCf < LR_DIM);
    assertTrue("slice param must be less than LR_DIM", sliceMaxCq < LR_DIM);
    Map<String,String> opts = new HashMap<>();
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    Range startsAtMinCf = new Range(new Key(LONG_LEX.encode(0l), LONG_LEX.encode(sliceMinCf), LONG_LEX.encode(sliceMinCq), new byte[] {}, Long.MAX_VALUE), null);
    loadKvs(foundKvs, opts, startsAtMinCf);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j > sliceMinCf && j <= sliceMaxCf && k > sliceMinCq && k <= sliceMaxCq) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
    foundKvs = new boolean[LR_DIM][LR_DIM][LR_DIM];
    sliceMinCq = 0;
    sliceMaxCq = 10;
    opts.put(CfCqSliceOpts.OPT_MIN_CF, new String(LONG_LEX.encode(sliceMinCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MIN_INCLUSIVE, "false");
    opts.put(CfCqSliceOpts.OPT_MIN_CQ, new String(LONG_LEX.encode(sliceMinCq), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CF, new String(LONG_LEX.encode(sliceMaxCf), UTF_8));
    opts.put(CfCqSliceOpts.OPT_MAX_CQ, new String(LONG_LEX.encode(sliceMaxCq), UTF_8));
    loadKvs(foundKvs, opts, INFINITY);
    for (int i = 0; i < LR_DIM; i++) {
      for (int j = 0; j < LR_DIM; j++) {
        for (int k = 0; k < LR_DIM; k++) {
          if (j > sliceMinCf && j <= sliceMaxCf && k > sliceMinCq && k <= sliceMaxCq) {
            assertTrue("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must be found in scan", foundKvs[i][j][k]);
          } else {
            assertFalse("(r, cf, cq) == (" + i + ", " + j + ", " + k + ") must not be found in scan", foundKvs[i][j][k]);
          }
        }
      }
    }
  }

  private void loadKvs(boolean[][][] foundKvs, Map<String,String> options, Range range) {
    loadKvs(new SortedMapIterator(data), foundKvs, options, range);
  }

  private void loadKvs(SortedKeyValueIterator<Key,Value> parent, boolean[][][] foundKvs, Map<String,String> options, Range range) {
    try {
      SortedKeyValueIterator<Key,Value> skvi = getFilterClass().newInstance();
      skvi.init(parent, options, null);
      skvi.seek(range, EMPTY_CF_SET, false);

      Random random = new Random();

      while (skvi.hasTop()) {
        Key k = skvi.getTopKey();
        int row = LONG_LEX.decode(k.getRow().copyBytes()).intValue();
        int cf = LONG_LEX.decode(k.getColumnFamily().copyBytes()).intValue();
        int cq = LONG_LEX.decode(k.getColumnQualifier().copyBytes()).intValue();

        assertFalse("Duplicate " + row + " " + cf + " " + cq, foundKvs[row][cf][cq]);
        foundKvs[row][cf][cq] = true;

        if (random.nextInt(100) == 0) {
          skvi.seek(new Range(k, false, range.getEndKey(), range.isEndKeyInclusive()), EMPTY_CF_SET, false);
        } else {
          skvi.next();
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Rows 0..(LR_DIM - 1) will each have LR_DIM CFs, each with LR_DIM CQs
   *
   * For instance if LR_DIM is 3, (cf,cq) r: val
   *
   * (0,0) (0,1) (0,2) (1,0) (1,1) (1,2) (2,0) (2,1) (2,2) 0 0 1 2 3 4 5 6 7 8 1 9 10 11 12 13 14 15 16 17 2 18 19 20 21 22 23 24 25 26
   */
  static TreeMap<Key,Value> createMap(int numRows, int numCfs, int numCqs) {
    TreeMap<Key,Value> data = new TreeMap<>();
    for (int i = 0; i < numRows; i++) {
      byte[] rowId = LONG_LEX.encode(ROW_ID_GEN.getAndIncrement());
      for (int j = 0; j < numCfs; j++) {
        for (int k = 0; k < numCqs; k++) {
          byte[] cf = LONG_LEX.encode((long) j);
          byte[] cq = LONG_LEX.encode((long) k);
          byte[] val = LONG_LEX.encode((long) (i * numCfs + j * numCqs + k));
          data.put(new Key(rowId, cf, cq, new byte[0], 9), new Value(val));
        }
      }
    }
    return data;
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
