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
package org.apache.accumulo.test.functional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class BloomFilterTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {
    
  }
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    ArrayList<TableSetup> tl = new ArrayList<TableSetup>();
    
    tl.add(new TableSetup("bt1"));
    tl.add(new TableSetup("bt2"));
    tl.add(new TableSetup("bt3"));
    tl.add(new TableSetup("bt4"));
    
    return tl;
  }
  
  @Override
  public void run() throws Exception {
    write("bt1", 1, 0, 1000000000, 1000);
    write("bt2", 2, 0, 1000000000, 1000);
    write("bt3", 3, 0, 1000000000, 1000);
    
    // test inserting an empty key
    BatchWriter bw = getConnector().createBatchWriter("bt4", new BatchWriterConfig());
    Mutation m = new Mutation(new Text(""));
    m.put(new Text(""), new Text(""), new Value("foo1".getBytes()));
    bw.addMutation(m);
    bw.close();
    getConnector().tableOperations().flush("bt4", null, null, true);
    
    for (String table : new String[]{"bt1", "bt2", "bt3"}) {
      getConnector().tableOperations().setProperty(table, Property.TABLE_INDEXCACHE_ENABLED.getKey(), "false");
      getConnector().tableOperations().setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "false");
      getConnector().tableOperations().flush(table, null, null, true);
      getConnector().tableOperations().compact(table, null, null, false, true);
    }
    
    // ensure compactions are finished
    super.checkRFiles("bt1", 1, 1, 1, 1);
    super.checkRFiles("bt2", 1, 1, 1, 1);
    super.checkRFiles("bt3", 1, 1, 1, 1);
    super.checkRFiles("bt4", 1, 1, 1, 1);
    
    // these queries should only run quickly if bloom filters are working, so lets get a base
    long t1 = query("bt1", 1, 0, 1000000000, 100000, 1000);
    long t2 = query("bt2", 2, 0, 1000000000, 100000, 1000);
    long t3 = query("bt3", 3, 0, 1000000000, 100000, 1000);
    
    getConnector().tableOperations().setProperty("bt1", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    getConnector().tableOperations().setProperty("bt1", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), "org.apache.accumulo.core.file.keyfunctor.RowFunctor");
    getConnector().tableOperations().compact("bt1", null, null, false, true);
    
    getConnector().tableOperations().setProperty("bt2", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    getConnector().tableOperations().setProperty("bt2", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(),
        "org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor");
    getConnector().tableOperations().compact("bt2", null, null, false, true);
    
    getConnector().tableOperations().setProperty("bt3", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    getConnector().tableOperations().setProperty("bt3", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(),
        "org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor");
    getConnector().tableOperations().compact("bt3", null, null, false, true);
    
    getConnector().tableOperations().setProperty("bt4", Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    getConnector().tableOperations().setProperty("bt4", Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), "org.apache.accumulo.core.file.keyfunctor.RowFunctor");
    getConnector().tableOperations().compact("bt4", null, null, false, true);
    
    // these queries should only run quickly if bloom
    // filters are working
    long tb1 = query("bt1", 1, 0, 1000000000, 100000, 1000);
    long tb2 = query("bt2", 2, 0, 1000000000, 100000, 1000);
    long tb3 = query("bt3", 3, 0, 1000000000, 100000, 1000);
    
    timeCheck(t1, tb1);
    timeCheck(t2, tb2);
    timeCheck(t3, tb3);
    
    // test querying for empty key
    Scanner scanner = getConnector().createScanner("bt4", Constants.NO_AUTHS);
    scanner.setRange(new Range(new Text("")));
    
    if (!scanner.iterator().next().getValue().toString().equals("foo1")) {
      throw new Exception("Did not see foo1");
    }
    
  }
  
  private void timeCheck(long t1, long t2) throws Exception {
    if (((t1 - t2) * 1.0 / t1) < .1) {
      throw new Exception("Queries had less than 10% improvement (old: " + t1 + " new: " + t2 + " improvement: " + ((t1 - t2) * 100. / t1) + "%)");
    }
  }
  
  private long query(String table, int depth, long start, long end, int num, int step) throws Exception {
    Random r = new Random(42);
    
    HashSet<Long> expected = new HashSet<Long>();
    List<Range> ranges = new ArrayList<Range>(num);
    Text key = new Text();
    Text row = new Text("row"), cq = new Text("cq"), cf = new Text("cf");
    
    for (int i = 0; i < num; ++i) {
      Long k = ((r.nextLong() & 0x7fffffffffffffffl) % (end - start)) + start;
      key.set(String.format("k_%010d", k));
      Range range = null;
      Key acuKey;
      
      if (k % (start + step) == 0) {
        expected.add(k);
      }
      
      switch (depth) {
        case 1:
          range = new Range(new Text(key));
          break;
        case 2:
          acuKey = new Key(row, key, cq);
          range = new Range(acuKey, true, acuKey.followingKey(PartialKey.ROW_COLFAM), false);
          break;
        case 3:
          acuKey = new Key(row, cf, key);
          range = new Range(acuKey, true, acuKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
          break;
      }
      
      ranges.add(range);
    }
    
    BatchScanner bs = getConnector().createBatchScanner(table, Constants.NO_AUTHS, 3);
    bs.setRanges(ranges);
    
    long t1 = System.currentTimeMillis();
    
    for (Entry<Key,Value> entry : bs) {
      long v = Long.parseLong(entry.getValue().toString());
      if (!expected.remove(v)) {
        throw new Exception("Got unexpected return " + entry.getKey() + " " + entry.getValue());
      }
    }
    
    long t2 = System.currentTimeMillis();
    
    if (expected.size() > 0) {
      throw new Exception("Did not get all expected values " + expected.size());
    }
    
    bs.close();
    
    return t2 - t1;
  }
  
  private void write(String table, int depth, long start, long end, int step) throws Exception {
    
    BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
    
    for (long i = start; i < end; i += step) {
      String key = String.format("k_%010d", i);
      
      Mutation m = null;
      
      switch (depth) {
        case 1:
          m = new Mutation(new Text(key));
          m.put(new Text("cf"), new Text("cq"), new Value(("" + i).getBytes()));
          break;
        case 2:
          m = new Mutation(new Text("row"));
          m.put(new Text(key), new Text("cq"), new Value(("" + i).getBytes()));
          break;
        case 3:
          m = new Mutation(new Text("row"));
          m.put(new Text("cf"), new Text(key), new Value(("" + i).getBytes()));
          break;
      }
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    getConnector().tableOperations().flush(table, null, null, true);
  }
}
