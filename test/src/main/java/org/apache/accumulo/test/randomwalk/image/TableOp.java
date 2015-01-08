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
package org.apache.accumulo.test.randomwalk.image;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class TableOp extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    // choose a table
    Random rand = new Random();
    String tableName;
    if (rand.nextInt(10) < 8) {
      tableName = state.getString("imageTableName");
    } else {
      tableName = state.getString("indexTableName");
    }

    // check if chosen table exists
    Connector conn = state.getConnector();
    TableOperations tableOps = conn.tableOperations();
    if (tableOps.exists(tableName) == false) {
      log.error("Table " + tableName + " does not exist!");
      return;
    }

    // choose a random action
    int num = rand.nextInt(10);
    if (num > 6) {
      log.debug("Retrieving info for " + tableName);
      tableOps.getLocalityGroups(tableName);
      tableOps.getProperties(tableName);
      tableOps.listSplits(tableName);
      tableOps.list();
    } else {
      log.debug("Clearing locator cache for " + tableName);
      tableOps.clearLocatorCache(tableName);
    }

    if (rand.nextInt(10) < 3) {
      Map<String,Set<Text>> groups = tableOps.getLocalityGroups(state.getString("imageTableName"));

      if (groups.size() == 0) {
        log.debug("Adding locality groups to " + state.getString("imageTableName"));
        groups = ImageFixture.getLocalityGroups();
      } else {
        log.debug("Removing locality groups from " + state.getString("imageTableName"));
        groups = new HashMap<String,Set<Text>>();
      }

      tableOps.setLocalityGroups(state.getString("imageTableName"), groups);
    }
  }
}
