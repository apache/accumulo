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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ConstraintTest extends FunctionalTest {
  
  @Override
  public void cleanup() {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    Map<String,String> config = parseConfig(Property.TABLE_CONSTRAINT_PREFIX + "1=org.apache.accumulo.examples.simple.constraints.NumericValueConstraint",
        Property.TABLE_CONSTRAINT_PREFIX + "2=org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint");
    return Arrays.asList(new TableSetup("ct", config), new TableSetup("ct2", config), new TableSetup("ct3", config));
  }
  
  @Override
  public void run() throws Exception {
    
    Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    logger.setLevel(Level.TRACE);
    
    test1();
    
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    logger.setLevel(Level.TRACE);
    
    test2("ct2", false);
    test2("ct3", true);
  }
  
  private void test1() throws Exception {
    BatchWriter bw = getConnector().createBatchWriter("ct", 100000, 60000l, 1);
    
    Mutation mut1 = new Mutation(new Text("r1"));
    mut1.put(new Text("cf1"), new Text("cq1"), new Value("123".getBytes()));
    
    bw.addMutation(mut1);
    
    // should not throw any exceptions
    bw.close();
    
    bw = getConnector().createBatchWriter("ct", 100000, 60000l, 1);
    
    // create a mutation with a non numeric value
    Mutation mut2 = new Mutation(new Text("r1"));
    mut2.put(new Text("cf1"), new Text("cq1"), new Value("123a".getBytes()));
    
    bw.addMutation(mut2);
    
    boolean sawMRE = false;
    
    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;
      
      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();
      
      if (cvsl.size() != 1) {
        throw new Exception("Unexpected constraints");
      }
      
      for (ConstraintViolationSummary cvs : cvsl) {
        if (!cvs.constrainClass.equals("org.apache.accumulo.examples.simple.constraints.NumericValueConstraint")) {
          throw new Exception("Unexpected constraint class " + cvs.constrainClass);
        }
        
        if (cvs.numberOfViolatingMutations != 1) {
          throw new Exception("Unexpected # violating mutations " + cvs.numberOfViolatingMutations);
        }
      }
    }
    
    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }
    
    // verify mutation did not go through
    Scanner scanner = getConnector().createScanner("ct", Constants.NO_AUTHS);
    scanner.setRange(new Range(new Text("r1")));
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Entry<Key,Value> entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    // remove the numeric value constraint
    getConnector().tableOperations().removeConstraint("ct", 1);
    UtilWaitThread.sleep(1000);
    
    // now should be able to add a non numeric value
    bw = getConnector().createBatchWriter("ct", 100000, 60000l, 1);
    bw.addMutation(mut2);
    bw.close();
    
    // verify mutation went through
    iter = scanner.iterator();
    entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123a".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    // add a constraint that references a non-existant class
    getConnector().tableOperations().setProperty("ct", Property.TABLE_CONSTRAINT_PREFIX + "1", "com.foobar.nonExistantClass");
    UtilWaitThread.sleep(1000);
    
    // add a mutation
    bw = getConnector().createBatchWriter("ct", 100000, 60000l, 1);
    
    Mutation mut3 = new Mutation(new Text("r1"));
    mut3.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes()));
    
    bw.addMutation(mut3);
    
    sawMRE = false;
    
    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, mutation went through when table had bad constraints");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;
    }
    
    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }
    
    // verify the mutation did not go through
    iter = scanner.iterator();
    entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123a".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    // remove the bad constraint
    getConnector().tableOperations().removeProperty("ct", Property.TABLE_CONSTRAINT_PREFIX + "1");
    UtilWaitThread.sleep(1000);
    
    // try the mutation again
    bw = getConnector().createBatchWriter("ct", 100000, 60000l, 1);
    bw.addMutation(mut3);
    bw.close();
    
    // verify it went through
    iter = scanner.iterator();
    entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("foo".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
  }
  
  private Mutation newMut(String row, String cf, String cq, String val) {
    Mutation mut1 = new Mutation(new Text(row));
    mut1.put(new Text(cf), new Text(cq), new Value(val.getBytes()));
    return mut1;
  }
  
  private void test2(String table, boolean doFlush) throws Exception {
    // test sending multiple mutations with multiple constrain violations... all of the non violating mutations
    // should go through
    int numericErrors = 2;
    
    BatchWriter bw = getConnector().createBatchWriter(table, 100000, 60000l, 1);
    bw.addMutation(newMut("r1", "cf1", "cq1", "123"));
    bw.addMutation(newMut("r1", "cf1", "cq2", "I'm a bad value"));
    if (doFlush) {
      try {
        bw.flush();
        throw new Exception("Didn't find a bad mutation");
      } catch (MutationsRejectedException mre) {
        // ignored
        try {
          bw.close();
        } catch (MutationsRejectedException ex) {
          // ignored
        }
        bw = getConnector().createBatchWriter(table, 100000, 60000l, 1);
        numericErrors = 1;
      }
    }
    bw.addMutation(newMut("r1", "cf1", "cq3", "I'm a naughty value"));
    bw.addMutation(newMut("@bad row@", "cf1", "cq2", "456"));
    bw.addMutation(newMut("r1", "cf1", "cq4", "789"));
    
    boolean sawMRE = false;
    
    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      System.out.println(mre);
      
      sawMRE = true;
      
      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();
      
      if (cvsl.size() != 2) {
        throw new Exception("Unexpected constraints");
      }
      
      HashMap<String,Integer> expected = new HashMap<String,Integer>();
      
      expected.put("org.apache.accumulo.examples.simple.constraints.NumericValueConstraint", numericErrors);
      expected.put("org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint", 1);
      
      for (ConstraintViolationSummary cvs : cvsl) {
        if (expected.get(cvs.constrainClass) != cvs.numberOfViolatingMutations) {
          throw new Exception("Unexpected " + cvs.constrainClass + " " + cvs.numberOfViolatingMutations);
        }
      }
    }
    
    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }
    
    Scanner scanner = getConnector().createScanner(table, Constants.NO_AUTHS);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    
    Entry<Key,Value> entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1")) || !entry.getValue().equals(new Value("123".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    entry = iter.next();
    
    if (!entry.getKey().getRow().equals(new Text("r1")) || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq4")) || !entry.getValue().equals(new Value("789".getBytes()))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }
    
    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception("Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
    
  }
  
}
