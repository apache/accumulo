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
package org.apache.accumulo.server.test.functional;

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
    
    // tl.add(new TableSetup("bt1",parseConfig(Property.TABLE_BLOOM_ENABLED+"=true", Property.TABLE_BLOOM_KEYDEPTH+"="+PartialKey.ROW.name())));
    // tl.add(new TableSetup("bt2",parseConfig(Property.TABLE_BLOOM_ENABLED+"=true", Property.TABLE_BLOOM_KEYDEPTH+"="+PartialKey.ROW_COLFAM.name())));
    // tl.add(new TableSetup("bt3",parseConfig(Property.TABLE_BLOOM_ENABLED+"=true",
    // Property.TABLE_BLOOM_KEYDEPTH+"="+PartialKey.ROW_COLFAM_COLQUAL.name())));
    // tl.add(new TableSetup("bt4",parseConfig(Property.TABLE_BLOOM_ENABLED+"=true", Property.TABLE_BLOOM_KEYDEPTH+"="+PartialKey.ROW.name())));
    tl.add(new TableSetup("bt1", parseConfig(Property.TABLE_BLOOM_ENABLED + "=true", Property.TABLE_BLOOM_KEY_FUNCTOR
        + "=org.apache.accumulo.core.file.keyfunctor.RowFunctor")));
    tl.add(new TableSetup("bt2", parseConfig(Property.TABLE_BLOOM_ENABLED + "=true", Property.TABLE_BLOOM_KEY_FUNCTOR
        + "=org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor")));
    tl.add(new TableSetup("bt3", parseConfig(Property.TABLE_BLOOM_ENABLED + "=true", Property.TABLE_BLOOM_KEY_FUNCTOR
        + "=org.apache.accumulo.core.file.keyfunctor.ColumnQualifierFunctor")));
    tl.add(new TableSetup("bt4", parseConfig(Property.TABLE_BLOOM_ENABLED + "=true", Property.TABLE_BLOOM_KEY_FUNCTOR
        + "=org.apache.accumulo.core.file.keyfunctor.RowFunctor")));
    return tl;
  }
  
  @Override
  public void run() throws Exception {
    write("bt1", 1, 0, 1000000000, 10000);
    write("bt2", 2, 0, 1000000000, 10000);
    write("bt3", 3, 0, 1000000000, 10000);
    
    // test inserting an empty key
    BatchWriter bw = getConnector().createBatchWriter("bt4", 1000000l, 60l, 3);
    Mutation m = new Mutation(new Text(""));
    m.put(new Text(""), new Text(""), new Value("foo1".getBytes()));
    bw.addMutation(m);
    bw.close();
    getConnector().tableOperations().flush("bt4", null, null, true);
    
    // ensure minor compactions are finished
    super.checkRFiles("bt1", 1, 1, 1, 1);
    super.checkRFiles("bt2", 1, 1, 1, 1);
    super.checkRFiles("bt3", 1, 1, 1, 1);
    super.checkRFiles("bt4", 1, 1, 1, 1);
    
    // these queries should only run quickly if bloom
    // filters are working
    query("bt1", 1, 0, 1000000000, 100000, 10000, 6);
    query("bt2", 2, 0, 1000000000, 100000, 10000, 6);
    query("bt3", 3, 0, 1000000000, 100000, 10000, 6);
    
    // test querying for empty key
    Scanner scanner = getConnector().createScanner("bt4", Constants.NO_AUTHS);
    scanner.setRange(new Range(new Text("")));
    
    if (!scanner.iterator().next().getValue().toString().equals("foo1")) {
      throw new Exception("Did not see foo1");
    }
    
  }
  
  private void query(String table, int depth, long start, long end, int num, int step, int secs) throws Exception {
    Random r = new Random(42);
    
    HashSet<Long> expected = new HashSet<Long>();
    List<Range> ranges = new ArrayList<Range>(num);
    Text key = new Text();
    Text row = new Text("row"), cq = new Text("cq"), cf = new Text("cf");
    
    for (int i = 0; i < num; ++i) {
      Long k = (Math.abs(r.nextLong()) % (end - start)) + start;
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
    
    if ((t2 - t1) / 1000.0 >= secs) {
      throw new Exception("Queries exceeded expected run time " + (t2 - t1) / 1000.0 + " " + secs);
    }
    
    bs.close();

  }
  
  private void write(String table, int depth, long start, long end, int step) throws Exception {
    
    BatchWriter bw = getConnector().createBatchWriter(table, 1000000l, 60l, 3);
    
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
