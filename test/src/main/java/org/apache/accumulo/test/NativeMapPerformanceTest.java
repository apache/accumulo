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
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.tserver.NativeMap;
import org.apache.hadoop.io.Text;

public class NativeMapPerformanceTest {

  private static final byte ROW_PREFIX[] = new byte[] {'r'};
  private static final byte COL_PREFIX[] = new byte[] {'c'};

  static Key newKey(int r, int c) {
    return new Key(new Text(FastFormat.toZeroPaddedString(r, 9, 10, ROW_PREFIX)), new Text(FastFormat.toZeroPaddedString(c, 6, 10, COL_PREFIX)));
  }

  static Mutation newMutation(int r) {
    return new Mutation(new Text(FastFormat.toZeroPaddedString(r, 9, 10, ROW_PREFIX)));
  }

  static Text ET = new Text();

  private static void pc(Mutation m, int c, Value v) {
    m.put(new Text(FastFormat.toZeroPaddedString(c, 6, 10, COL_PREFIX)), ET, Long.MAX_VALUE, v);
  }

  static void runPerformanceTest(int numRows, int numCols, int numLookups, String mapType) {

    SortedMap<Key,Value> tm = null;
    NativeMap nm = null;

    if (mapType.equals("SKIP_LIST"))
      tm = new ConcurrentSkipListMap<>();
    else if (mapType.equals("TREE_MAP"))
      tm = Collections.synchronizedSortedMap(new TreeMap<Key,Value>());
    else if (mapType.equals("NATIVE_MAP"))
      nm = new NativeMap();
    else
      throw new IllegalArgumentException(" map type must be SKIP_LIST, TREE_MAP, or NATIVE_MAP");

    Random rand = new Random(19);

    // puts
    long tps = System.currentTimeMillis();

    if (nm != null) {
      for (int i = 0; i < numRows; i++) {
        int row = rand.nextInt(1000000000);
        Mutation m = newMutation(row);
        for (int j = 0; j < numCols; j++) {
          int col = rand.nextInt(1000000);
          Value val = new Value("test".getBytes(UTF_8));
          pc(m, col, val);
        }
        nm.mutate(m, i);
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        int row = rand.nextInt(1000000000);
        for (int j = 0; j < numCols; j++) {
          int col = rand.nextInt(1000000);
          Key key = newKey(row, col);
          Value val = new Value("test".getBytes(UTF_8));
          tm.put(key, val);
        }
      }
    }

    long tpe = System.currentTimeMillis();

    // Iteration
    Iterator<Entry<Key,Value>> iter;
    if (nm != null) {
      iter = nm.iterator();
    } else {
      iter = tm.entrySet().iterator();
    }

    long tis = System.currentTimeMillis();

    while (iter.hasNext()) {
      iter.next();
    }

    long tie = System.currentTimeMillis();

    rand = new Random(19);
    int rowsToLookup[] = new int[numLookups];
    int colsToLookup[] = new int[numLookups];
    for (int i = 0; i < Math.min(numLookups, numRows); i++) {
      int row = rand.nextInt(1000000000);
      int col = -1;
      for (int j = 0; j < numCols; j++) {
        col = rand.nextInt(1000000);
      }

      rowsToLookup[i] = row;
      colsToLookup[i] = col;
    }

    // get

    long tgs = System.currentTimeMillis();
    if (nm != null) {
      for (int i = 0; i < numLookups; i++) {
        Key key = newKey(rowsToLookup[i], colsToLookup[i]);
        if (nm.get(key) == null) {
          throw new RuntimeException("Did not find " + rowsToLookup[i] + " " + colsToLookup[i] + " " + i);
        }
      }
    } else {
      for (int i = 0; i < numLookups; i++) {
        Key key = newKey(rowsToLookup[i], colsToLookup[i]);
        if (tm.get(key) == null) {
          throw new RuntimeException("Did not find " + rowsToLookup[i] + " " + colsToLookup[i] + " " + i);
        }
      }
    }
    long tge = System.currentTimeMillis();

    long memUsed = 0;
    if (nm != null) {
      memUsed = nm.getMemoryUsed();
    }

    int size = (nm == null ? tm.size() : nm.size());

    // delete
    long tds = System.currentTimeMillis();

    if (nm != null)
      nm.delete();

    long tde = System.currentTimeMillis();

    if (tm != null)
      tm.clear();

    System.gc();
    System.gc();
    System.gc();
    System.gc();

    sleepUninterruptibly(3, TimeUnit.SECONDS);

    System.out.printf("mapType:%10s   put rate:%,6.2f  scan rate:%,6.2f  get rate:%,6.2f  delete time : %6.2f  mem : %,d%n", "" + mapType, (numRows * numCols)
        / ((tpe - tps) / 1000.0), (size) / ((tie - tis) / 1000.0), numLookups / ((tge - tgs) / 1000.0), (tde - tds) / 1000.0, memUsed);

  }

  public static void main(String[] args) {

    if (args.length != 3) {
      throw new IllegalArgumentException("Usage : " + NativeMapPerformanceTest.class.getName() + " <map type> <rows> <columns>");
    }

    String mapType = args[0];
    int rows = Integer.parseInt(args[1]);
    int cols = Integer.parseInt(args[2]);

    runPerformanceTest(rows, cols, 10000, mapType);
    runPerformanceTest(rows, cols, 10000, mapType);
    runPerformanceTest(rows, cols, 10000, mapType);

  }

}
