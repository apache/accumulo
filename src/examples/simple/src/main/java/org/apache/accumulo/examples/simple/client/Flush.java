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
package org.apache.accumulo.examples.simple.client;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Simple example for using tableOperations() (like create, delete, flush, etc).
 */
public class Flush {
  
  public static void main(String[] args) {
    if (args.length != 5) {
      System.err.println("Usage: accumulo accumulo.examples.client.Flush <instance name> <zoo keepers> <username> <password> <tableName>");
      return;
    }
    String instanceName = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    String password = args[3];
    String table = args[4];
    
    Connector connector;
    try {
      ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
      connector = instance.getConnector(user, password.getBytes());
      connector.tableOperations().flush(table, null, null, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
