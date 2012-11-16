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
package org.apache.accumulo.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class MiniAccumuloClusterTest {
  
  public static TemporaryFolder folder = new TemporaryFolder();
  
  private static MiniAccumuloCluster accumulo;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {

    folder.create();
    
    accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret", new HashMap<String,String>());
    
    accumulo.start();
    
  }

  @Test(timeout = 30000)
  public void test() throws Exception {
    Connector conn = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZookeepers()).getConnector("root", "superSecret");
    
    conn.tableOperations().create("table1");
    
    conn.securityOperations().createUser("user1", "pass1".getBytes(), new Authorizations("A", "B"));
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.WRITE);
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.READ);
    
    IteratorSetting is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("META", "COUNT")));
    
    conn.tableOperations().attachIterator("table1", is);

    Connector uconn = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZookeepers()).getConnector("user1", "pass1");
    
    BatchWriter bw = uconn.createBatchWriter("table1", new BatchWriterConfig());
    
    UUID uuid = UUID.randomUUID();
    
    Mutation m = new Mutation(uuid.toString());
    m.put("META", "SIZE", new ColumnVisibility("A|B"), "8");
    m.put("META", "CRC", new ColumnVisibility("A|B"), "456");
    m.put("META", "COUNT", new ColumnVisibility("A|B"), "1");
    m.put("DATA", "IMG", new ColumnVisibility("A&B"), "ABCDEFGH");
    
    bw.addMutation(m);
    bw.flush();
    
    m = new Mutation(uuid.toString());
    m.put("META", "COUNT", new ColumnVisibility("A|B"), "1");
    m.put("META", "CRC", new ColumnVisibility("A|B"), "123");
    bw.addMutation(m);

    bw.close();
    
    int count = 0;
    Scanner scanner = uconn.createScanner("table1", new Authorizations("A"));
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifierData().toString().equals("COUNT")) {
        Assert.assertEquals("2", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("SIZE")) {
        Assert.assertEquals("8", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("CRC")) {
        Assert.assertEquals("123", entry.getValue().toString());
      } else {
        Assert.assertTrue(false);
      }
      count++;
    }
    
    Assert.assertEquals(3, count);
    
    count = 0;
    scanner = uconn.createScanner("table1", new Authorizations("A", "B"));
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifierData().toString().equals("IMG")) {
        Assert.assertEquals("ABCDEFGH", entry.getValue().toString());
      }
      count++;
    }
    
    Assert.assertEquals(4, count);
    
    conn.tableOperations().delete("table1");
  }
  
  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
}
