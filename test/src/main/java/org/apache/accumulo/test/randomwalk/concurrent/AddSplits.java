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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class AddSplits extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    Random rand = (Random) state.get("rand");

    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");
    tableNames = new ArrayList<String>(tableNames);
    tableNames.add(MetadataTable.NAME);
    String tableName = tableNames.get(rand.nextInt(tableNames.size()));

    TreeSet<Text> splits = new TreeSet<Text>();

    for (int i = 0; i < rand.nextInt(10) + 1; i++)
      splits.add(new Text(String.format("%016x", rand.nextLong() & 0x7fffffffffffffffl)));

    try {
      conn.tableOperations().addSplits(tableName, splits);
      log.debug("Added " + splits.size() + " splits " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("AddSplits " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException e) {
      log.debug("AddSplits " + tableName + " failed, offline");
    }
  }
}
