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
package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class OfflineTable extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    Random rand = (Random) state.get("rand");

    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");

    String tableName = tableNames.get(rand.nextInt(tableNames.size()));

    try {
      conn.tableOperations().offline(tableName, rand.nextBoolean());
      log.debug("Offlined " + tableName);
      UtilWaitThread.sleep(rand.nextInt(200));
      conn.tableOperations().online(tableName, rand.nextBoolean());
      log.debug("Onlined " + tableName);
    } catch (TableNotFoundException tne) {
      log.debug("offline or online failed " + tableName + ", doesnt exist");
    }

  }
}
