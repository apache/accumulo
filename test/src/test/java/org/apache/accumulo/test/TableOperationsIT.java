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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
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
    
    ZooKeeperInstance instance = new ZooKeeperInstance(accumuloCluster.getInstanceName(), accumuloCluster.getZooKeepers());
    connector = instance.getConnector(ROOT, new PasswordToken(ROOT_PASS.getBytes()));
  }
  
  @Test
  public void getDiskUsage() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TException {
    connector.tableOperations().create("table1");
    List<DiskUsage> diskUsage = connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
    assertEquals(1, diskUsage.size());
    assertEquals(0, (long) diskUsage.get(0).getUsage());
    assertEquals("table1", diskUsage.get(0).getTables().iterator().next());

    connector.securityOperations().revokeTablePermission(ROOT, "table1", TablePermission.READ);
    try {
      connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
      fail("Should throw securityexception");
    } catch(AccumuloSecurityException e) {}

    connector.tableOperations().delete("table1");
    try {
      connector.tableOperations().getDiskUsage(Collections.singleton("table1"));
      fail("Should throw tablenotfound");
    } catch (TableNotFoundException e) {}
  }
  
  @AfterClass
  public static void shutDown() throws IOException, InterruptedException {
    accumuloCluster.stop();
    tempFolder.delete();
  }
}
