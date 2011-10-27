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

import java.io.IOException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ScanCache;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public class TestScanCache2 {
  
  public static void main() throws IOException {
    
    SortedMap<Key,Value> m = new TreeMap<Key,Value>();
    
    Random r = new Random();
    final Text security = new Text();
    final Text columnf = new Text("colfam");
    final Text columnq = new Text("colqual");
    for (int i = 2; i <= 2000000; i += 2) {
      final Text row = new Text(String.format("row_%06d", i));
      final Text value = new Text("value" + i);
      final Key key = new Key(row, columnf, columnq, security, System.currentTimeMillis());
      final byte[] valBytes = TextUtil.getBytes(value);
      final Value dibw = new Value(valBytes);
      m.put(key, dibw);
    }
    
    System.out.println("created map of size " + m.size());
    
    SortedMapIterator smi = new SortedMapIterator(m);
    
    KeyExtent extent = new KeyExtent(new Text("tablename"), null, null);
    
    ScanCache sc = new ScanCache(10000000, extent);
    
    // test correctness of the scan cache
    
    for (int i = 1; i <= 100000; i++) {
      sc.setAuthorityIterator(smi);
      int startRow = Math.abs(r.nextInt() % 2000000);
      boolean skipStartRow = false;// r.nextBoolean();
      if (i % 7 == 0) {
        Text row = new Text(String.format("row_%06d", startRow + 1));
        Key k = new Key(row, columnf, columnq, security, System.currentTimeMillis());
        final byte[] valBytes = ("added value " + k.toString()).getBytes();
        final Value dibw = new Value(valBytes);
        m.put(k, dibw);
        sc.invalidate(k);
        System.out.println("Added row " + k + ", " + dibw);
      }
      if (skipStartRow) {
        System.out.println("Scanning after " + startRow);
      } else {
        System.out.println("Scanning from " + startRow);
      }
      Key startKey = new Key(new Text(String.format("row_%06d", startRow)), columnf, columnq, Long.MAX_VALUE);
      sc.seek(new Range(startKey, null), LocalityGroupUtil.EMPTY_CF_SET, false);
      for (int j = 0; j < 20000; j++) {
        // scan through the cache
        if (!sc.hasTop()) {
          System.out.println("end of cache reached with key=" + startKey + " and j=" + j);
          break;
        }
        if (j == 0)
          System.out.println("first key: " + sc.getTopKey() + "  value: " + sc.getTopValue());
        try {
          sc.next();
        } catch (IOException e) {
          throw e;
        }
      }
      if (sc.hasTop()) {
        System.out.println("last key: " + sc.getTopKey() + "  value: " + sc.getTopValue());
      }
      sc.finishScan();
    }
    
    System.out.println("now scanning from the beginning of the table");
    Text firstRow = new Text("");// new Text(String.format("row_%06d", 0));
    Key firstKey = new Key(firstRow, columnf, columnq, Long.MAX_VALUE);
    // test scanning from the beginning of the table
    System.out.println("smi has " + m.size() + " entries");
    for (int i = 0; i < 1000; i++) {
      System.out.println("Scan " + (i + 1));
      sc.setAuthorityIterator(smi);
      sc.seek(new Range(firstKey, null), LocalityGroupUtil.EMPTY_CF_SET, false);
      Key lastKey = null;
      for (int j = 0; j < (i + 1) * 1000 && j < 100000; j++) {
        if (!sc.hasTop()) {
          System.out.println("finished at row " + j);
          break;
        }
        if (j == 0) {
          System.out.println("first key: " + sc.getTopKey());
        }
        lastKey = sc.getTopKey();
        try {
          sc.next();
        } catch (IOException e) {
          e.printStackTrace();
          break;
        }
      }
      System.out.println("last key: " + lastKey);
      sc.finishScan();
    }
  }
}
