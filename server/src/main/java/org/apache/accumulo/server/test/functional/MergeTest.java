/**
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * 
 */
public class MergeTest extends FunctionalTest {
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.emptyList();
  }
  
  
  private String[] ns(String...strings){
    return strings;
  }
  
  @Override
  public void run() throws Exception {
    int tc = 0;
    
    runMergeTest("foo" + tc++, ns(), ns(), ns("l", "m", "n"), ns(null, "l"), ns(null, "n"));

    runMergeTest("foo" + tc++, ns("m"), ns(), ns("l", "m", "n"), ns(null, "l"), ns(null, "n"));
    runMergeTest("foo" + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns("m", "n"), ns(null, "z"));
    runMergeTest("foo" + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns(null, "b"), ns("l", "m"));
    
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns(), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns(null, "s"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("c", "m"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("n", "r"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns(null, "s"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "m"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns(null, "s"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("q", "r"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("aa", "b"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("r", "s"), ns(null, "z"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("l", "m"));
    runMergeTest("foo" + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns("q", "r"));
    
  }
  
  private void runMergeTest(String table, String[] splits, String[] expectedSplits, String[] inserts, String[] start, String[] end) throws Exception {
    int count = 0;
    
    for (String s : start) {
      for (String e : end) {
        runMergeTest(table + "_" + count++, splits, expectedSplits, inserts, s, e);
      }
    }
  }
  
  private void runMergeTest(String table, String[] splits, String[] expectedSplits, String[] inserts, String start, String end) throws Exception {
    System.out.println("Running merge test " + table + " " + Arrays.asList(splits) + " " + start + " " + end);

    Connector conn = super.getConnector();
    conn.tableOperations().create(table, true, TimeType.LOGICAL);
    TreeSet<Text> splitSet = new TreeSet<Text>();
    for (String split : splits) {
      splitSet.add(new Text(split));
    }
    conn.tableOperations().addSplits(table, splitSet);
    
    BatchWriter bw = conn.createBatchWriter(table, 1000000, 60000l, 1);
    HashSet<String> expected = new HashSet<String>();
    for (String row : inserts) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", row);
      bw.addMutation(m);
      expected.add(row);
    }
    
    bw.close();
    
    conn.tableOperations().merge(table, start == null ? null : new Text(start), end == null ? null : new Text(end));
    
    Scanner scanner = conn.createScanner(table, Constants.NO_AUTHS);
    
    HashSet<String> observed = new HashSet<String>();
    for (Entry<Key,Value> entry : scanner) {
      String row = entry.getKey().getRowData().toString();
      if (!observed.add(row)) {
        throw new Exception("Saw data twice " + table + " " + row);
      }
    }
    
    if (!observed.equals(expected)) {
      throw new Exception("data inconsistency " + table + " " + observed + " != " + expected);
    }
    
    HashSet<Text> currentSplits = new HashSet<Text>(conn.tableOperations().getSplits(table));
    HashSet<Text> ess = new HashSet<Text>();
    for (String es : expectedSplits) {
      ess.add(new Text(es));
    }
    
    if (!currentSplits.equals(ess)) {
      throw new Exception("split inconsistency " + table + " " + currentSplits + " != " + ess);
    }
  }
  
  @Override
  public void cleanup() throws Exception {}
  
}
