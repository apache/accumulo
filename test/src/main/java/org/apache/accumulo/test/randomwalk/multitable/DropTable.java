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
import java.util.Random;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class DropTable extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    @SuppressWarnings("unchecked")
    ArrayList<String> tables = (ArrayList<String>) state.get("tableList");

    // don't drop a table if we only have one table or less
    if (tables.size() <= 1) {
      return;
    }

    Random rand = new Random();
    String tableName = tables.remove(rand.nextInt(tables.size()));

    try {
      state.getConnector().tableOperations().delete(tableName);
      log.debug("Dropped " + tableName);
    } catch (TableNotFoundException e) {
      log.error("Tried to drop table " + tableName + " but could not be found!");
    }
  }
}
