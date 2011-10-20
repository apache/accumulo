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
package org.apache.accumulo.server.test.randomwalk.multitable;

import java.net.InetAddress;
import java.util.ArrayList;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;

public class MultiTableFixture extends Fixture {
  
  @Override
  public void setUp(State state) throws Exception {
    
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    
    state.set("tableNamePrefix", String.format("multi_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis()));
    state.set("nextId", new Integer(0));
    state.set("numWrites", new Integer(0));
    state.set("totalWrites", new Integer(0));
    state.set("tableList", new ArrayList<String>());
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    
    Connector conn = state.getConnector();
    
    @SuppressWarnings("unchecked")
    ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
    
    for (String tableName : tables) {
      try {
        conn.tableOperations().delete(tableName);
        log.debug("Dropping table " + tableName);
      } catch (TableNotFoundException e) {
        log.warn("Tried to drop table " + tableName + " but could not be found!");
      }
    }
  }
}
