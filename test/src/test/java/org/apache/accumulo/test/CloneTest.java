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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class CloneTest {
  
  public static TemporaryFolder folder = new TemporaryFolder();
  private MiniAccumuloCluster accumulo;
  private String secret = "secret";
  
  @Before
  public void setUp() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  }
  
  @After
  public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
  @Test(timeout = 120 * 1000)
  public void run() throws Exception {
    String table1 = "clone1";
    String table2 = "clone2";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector c = zki.getConnector("root", new PasswordToken(secret));
    
    c.tableOperations().create(table1);
    
    c.tableOperations().setProperty(table1, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1M");
    c.tableOperations().setProperty(table1, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "2M");
    c.tableOperations().setProperty(table1, Property.TABLE_FILE_MAX.getKey(), "23");
    
    BatchWriter bw = c.createBatchWriter(table1, new BatchWriterConfig());
    
    Mutation m1 = new Mutation("001");
    m1.put("data", "x", "9");
    m1.put("data", "y", "7");
    
    Mutation m2 = new Mutation("008");
    m2.put("data", "x", "3");
    m2.put("data", "y", "4");
    
    bw.addMutation(m1);
    bw.addMutation(m2);
    
    bw.flush();
    
    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "500K");
    
    Set<String> exclude = new HashSet<String>();
    exclude.add(Property.TABLE_FILE_MAX.getKey());
    
    c.tableOperations().clone(table1, table2, true, props, exclude);
    
    Mutation m3 = new Mutation("009");
    m3.put("data", "x", "1");
    m3.put("data", "y", "2");
    bw.addMutation(m3);
    bw.close();

    Scanner scanner = c.createScanner(table2, Constants.NO_AUTHS);
    
    HashMap<String,String> expected = new HashMap<String,String>();
    expected.put("001:x", "9");
    expected.put("001:y", "7");
    expected.put("008:x", "3");
    expected.put("008:y", "4");
    
    HashMap<String,String> actual = new HashMap<String,String>();
    
    for (Entry<Key,Value> entry : scanner)
      actual.put(entry.getKey().getRowData().toString() + ":" + entry.getKey().getColumnQualifierData().toString(), entry.getValue().toString());
    
    Assert.assertEquals(expected, actual);
    
    HashMap<String,String> tableProps = new HashMap<String,String>();
    for (Entry<String,String> prop : c.tableOperations().getProperties(table2)) {
      tableProps.put(prop.getKey(), prop.getValue());
    }

    Assert.assertEquals("500K", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey()));
    Assert.assertEquals(Property.TABLE_FILE_MAX.getDefaultValue(), tableProps.get(Property.TABLE_FILE_MAX.getKey()));
    Assert.assertEquals("2M", tableProps.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey()));
    
  }
}
