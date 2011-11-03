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
package org.apache.accumulo.server.test.randomwalk.sequential;

import java.net.InetAddress;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;

public class SequentialFixture extends Fixture {
  
  String seqTableName;
  
  @Override
  public void setUp(State state) throws Exception {
    
    Connector conn = state.getConnector();
    Instance instance = state.getInstance();
    
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    
    seqTableName = String.format("sequential_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis());
    state.set("seqTableName", seqTableName);
    
    try {
      conn.tableOperations().create(seqTableName);
      log.debug("Created table " + seqTableName + " (id:" + Tables.getNameToIdMap(instance).get(seqTableName) + ")");
    } catch (TableExistsException e) {
      log.warn("Table " + seqTableName + " already exists!");
      throw e;
    }
    conn.tableOperations().setProperty(seqTableName, "table.scan.max.memory", "1K");
    
    state.set("numWrites", new Integer(0));
    state.set("totalWrites", new Integer(0));
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    
    log.debug("Dropping tables: " + seqTableName);
    
    Connector conn = state.getConnector();
    
    conn.tableOperations().delete(seqTableName);
  }
}
