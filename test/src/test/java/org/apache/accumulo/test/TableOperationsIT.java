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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableOperationsIT {
  
  static TemporaryFolder tempFolder = new TemporaryFolder();
  static final String ROOT = "root";
  static final String ROOT_PASS = "password";
  
  static MiniAccumuloCluster accumuloCluster;
  
  static Connector connector;
  static TabletClientService.Client client;
  
  @BeforeClass
  public static void startUp() throws IOException, AccumuloException, AccumuloSecurityException, TTransportException, InterruptedException {
    tempFolder.create();
    accumuloCluster = new MiniAccumuloCluster(tempFolder.getRoot(), ROOT_PASS);
    
    accumuloCluster.start();
    
    connector = accumuloCluster.getConnector(ROOT, ROOT_PASS);
  }
  
  @Test(timeout=30*1000)
  public void getDiskUsageErrors() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TException {
    connector.tableOperations().create("table1");
    List<DiskUsage> diskUsage = connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
    assertEquals(1, diskUsage.size());
    assertEquals(0, (long) diskUsage.get(0).getUsage());
    assertEquals("table1", diskUsage.get(0).getTables().iterator().next());
    
    connector.securityOperations().revokeTablePermission(ROOT, "table1", TablePermission.READ);
    try {
      connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
      fail("Should throw securityexception");
    } catch (AccumuloSecurityException e) {}
    
    connector.tableOperations().delete("table1");
    try {
      connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
      fail("Should throw tablenotfound");
    } catch (TableNotFoundException e) {}
  }
  
  @Test(timeout=30*1000)
  public void getDiskUsage() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TException {
    
    connector.tableOperations().create("table1");
    
    // verify 0 disk usage
    List<DiskUsage> diskUsages = connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
    assertEquals(1, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertEquals(new Long(0), diskUsages.get(0).getUsage());
    assertEquals("table1", diskUsages.get(0).getTables().first());
    
    // add some data
    BatchWriter bw = connector.createBatchWriter("table1", new BatchWriterConfig());
    Mutation m = new Mutation("a");
    m.put("b", "c", new Value("abcde".getBytes()));
    bw.addMutation(m);
    bw.flush();
    bw.close();
    
    connector.tableOperations().compact("table1", new Text("A"), new Text("z"), true, true);
    
    // verify we have usage
    diskUsages = connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
    assertEquals(1, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);
    assertEquals("table1", diskUsages.get(0).getTables().first());
    
    // clone table
    connector.tableOperations().clone("table1", "table2", false, null, null);
    
    // verify tables are exactly the same
    Set<String> tables = new HashSet<String>();
    tables.add("table1");
    tables.add("table2");
    diskUsages = connector.tableOperations().getDiskUsage(tables);
    assertEquals(1, diskUsages.size());
    assertEquals(2, diskUsages.get(0).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);
    
    connector.tableOperations().compact("table1", new Text("A"), new Text("z"), true, true);
    connector.tableOperations().compact("table2", new Text("A"), new Text("z"), true, true);
    
    // verify tables have differences
    diskUsages = connector.tableOperations().getDiskUsage(tables);
    assertEquals(2, diskUsages.size());
    assertEquals(1, diskUsages.get(0).getTables().size());
    assertEquals(1, diskUsages.get(1).getTables().size());
    assertTrue(diskUsages.get(0).getUsage() > 0);
    assertTrue(diskUsages.get(1).getUsage() > 0);
    
    connector.tableOperations().delete("table1");
  }
  
  @Test(timeout=30*1000)
  public void createTable() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    connector.tableOperations().create("table1");
    Iterable<Map.Entry<String,String>> itrProps = connector.tableOperations().getProperties("table1");
    Map<String,String> props = propsToMap(itrProps);
    assertEquals(DefaultKeySizeConstraint.class.getName(), props.get(Property.TABLE_CONSTRAINT_PREFIX.toString() + "1"));
    connector.tableOperations().delete("table1");
  }
  
  private Map<String,String> propsToMap(Iterable<Map.Entry<String,String>> props) {
    Map<String,String> map = new HashMap<String,String>();
    for (Map.Entry<String,String> prop : props) {
      map.put(prop.getKey(), prop.getValue());
    }
    return map;
  }
  
  @AfterClass
  public static void shutDown() throws IOException, InterruptedException {
    accumuloCluster.stop();
    tempFolder.delete();
  }
}
