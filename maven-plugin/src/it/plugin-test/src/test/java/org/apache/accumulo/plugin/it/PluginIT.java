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
package org.apache.accumulo.plugin.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.accumulo.plugin.CustomFilter;
import org.junit.BeforeClass;
import org.junit.Test;

public class PluginIT {

  private static Instance instance;
  private static Connector connector;

  @BeforeClass
  public static void setUp() throws Exception {
    String instanceName = "plugin-it-instance";
    instance = new MiniAccumuloInstance(instanceName, new File("target/accumulo-maven-plugin/" + instanceName));
    connector = instance.getConnector("root", new PasswordToken("ITSecret"));
  }

  @Test
  public void testInstanceConnection() {
    assertTrue(instance != null);
    assertTrue(instance instanceof MiniAccumuloInstance);
    assertTrue(connector != null);
    assertTrue(connector instanceof Connector);
  }

  @Test
  public void testCreateTable() throws AccumuloException, AccumuloSecurityException, TableExistsException, IOException {
    String tableName = "testCreateTable";
    connector.tableOperations().create(tableName);
    assertTrue(connector.tableOperations().exists(tableName));
    assertTrue(new File("target/accumulo-maven-plugin/" + instance.getInstanceName() + "/testCreateTablePassed").createNewFile());
  }

  @Test
  public void writeToTable() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
    String tableName = "writeToTable";
    connector.tableOperations().create(tableName);
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("ROW");
    m.put("CF", "CQ", "V");
    bw.addMutation(m);
    bw.close();
    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      count++;
      assertEquals("ROW", entry.getKey().getRow().toString());
      assertEquals("CF", entry.getKey().getColumnFamily().toString());
      assertEquals("CQ", entry.getKey().getColumnQualifier().toString());
      assertEquals("V", entry.getValue().toString());
    }
    assertEquals(1, count);
    assertTrue(new File("target/accumulo-maven-plugin/" + instance.getInstanceName() + "/testWriteToTablePassed").createNewFile());
  }

  @Test
  public void checkIterator() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    String tableName = "checkIterator";
    connector.tableOperations().create(tableName);
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("ROW1");
    m.put("allowed", "CQ1", "V1");
    m.put("denied", "CQ2", "V2");
    m.put("allowed", "CQ3", "V3");
    bw.addMutation(m);
    m = new Mutation("ROW2");
    m.put("allowed", "CQ1", "V1");
    m.put("denied", "CQ2", "V2");
    m.put("allowed", "CQ3", "V3");
    bw.addMutation(m);
    bw.close();

    // check filter
    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    IteratorSetting is = new IteratorSetting(5, CustomFilter.class);
    scanner.addScanIterator(is);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      count++;
      assertEquals("allowed", entry.getKey().getColumnFamily().toString());
    }
    assertEquals(4, count);

    // check filter negated
    scanner.clearScanIterators();
    CustomFilter.setNegate(is, true);
    scanner.addScanIterator(is);
    count = 0;
    for (Entry<Key,Value> entry : scanner) {
      count++;
      assertEquals("denied", entry.getKey().getColumnFamily().toString());
    }
    assertEquals(2, count);
    assertTrue(new File("target/accumulo-maven-plugin/" + instance.getInstanceName() + "/testCheckIteratorPassed").createNewFile());
  }

}
