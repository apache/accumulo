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
package org.apache.accumulo.test.randomwalk.multitable;

import java.util.ArrayList;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class CreateTable extends Test {

  private final TreeSet<Text> splits;

  public CreateTable() {
    splits = new TreeSet<Text>();
    for (int i = 1; i < 10; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
  }

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    int nextId = ((Integer) state.get("nextId")).intValue();
    String tableName = String.format("%s_%d", state.getString("tableNamePrefix"), nextId);
    try {
      conn.tableOperations().create(tableName);
      String tableId = Tables.getNameToIdMap(state.getInstance()).get(tableName);
      log.debug("created " + tableName + " (id:" + tableId + ")");
      // Add some splits to make the server's life easier
      conn.tableOperations().addSplits(tableName, splits);
      log.debug("created " + splits.size() + " splits on " + tableName);
      @SuppressWarnings("unchecked")
      ArrayList<String> tables = (ArrayList<String>) state.get("tableList");
      tables.add(tableName);
    } catch (TableExistsException e) {
      log.warn("Failed to create " + tableName + " as it already exists");
    }

    nextId++;
    state.set("nextId", Integer.valueOf(nextId));
  }
}
