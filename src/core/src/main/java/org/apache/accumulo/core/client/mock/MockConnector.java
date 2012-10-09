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
package org.apache.accumulo.core.client.mock;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;

public class MockConnector extends Connector {
  
  String username;
  private final MockAccumulo acu;
  private final Instance instance;
  
  MockConnector(String username, Instance instance) {
    this(username, new MockAccumulo(), instance);
  }
  
  @SuppressWarnings("deprecation")
  MockConnector(String username, MockAccumulo acu, Instance instance) {
    this.username = username;
    this.acu = acu;
    this.instance = instance;
  }
  
  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    return acu.createBatchScanner(tableName, authorizations);
  }
  
  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    return new MockBatchDeleter(acu, tableName, authorizations);
  }
  
  @Override
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    return new MockBatchWriter(acu, tableName);
  }
  
  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return new MockMultiTableBatchWriter(acu);
  }
  
  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    MockTable table = acu.tables.get(tableName);
    if (table == null)
      throw new TableNotFoundException(tableName, tableName, "no such table");
    return new MockScanner(table, authorizations);
  }
  
  @Override
  public Instance getInstance() {
    return instance;
  }
  
  @Override
  public String whoami() {
    return username;
  }
  
  @Override
  public TableOperations tableOperations() {
    return new MockTableOperations(acu, username);
  }
  
  @Override
  public SecurityOperations securityOperations() {
    return new MockSecurityOperations(acu);
  }
  
  @Override
  public InstanceOperations instanceOperations() {
    return new MockInstanceOperations(acu);
  }
  
}
