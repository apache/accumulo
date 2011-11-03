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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

public class MockAccumulo {
  final Map<String,MockTable> tables = new HashMap<String,MockTable>();
  final Map<String,String> systemProperties = new HashMap<String,String>();
  Map<String,MockUser> users = new HashMap<String,MockUser>();
  
  {
    MockUser root = new MockUser("root", new byte[] {}, Constants.NO_AUTHS);
    root.permissions.add(SystemPermission.SYSTEM);
    users.put(root.name, root);
    createTable("root", Constants.METADATA_TABLE_NAME);
  }
  
  void setProperty(String key, String value) {
    systemProperties.put(key, value);
  }
  
  public void addMutation(String table, Mutation m) {
    MockTable t = tables.get(table);
    t.addMutation(m);
  }
  
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations) {
    return new MockBatchScanner(tables.get(tableName), authorizations);
  }
  
  public void createTable(String username, String tableName) {
    MockTable t = new MockTable();
    t.userPermissions.put(username, EnumSet.allOf(TablePermission.class));
    tables.put(tableName, t);
  }
  
  public void addAggregators(String tableName, List<AggregatorConfiguration> aggregators) {
    tables.get(tableName).addAggregators(aggregators);
  }
}
