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
package org.apache.accumulo.test.randomwalk.sequential;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.util.ToolRunner;

public class MapRedVerify extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    String[] args = new String[8];
    args[0] = "-libjars";
    args[1] = state.getMapReduceJars();
    args[2] = state.getProperty("USERNAME");
    args[3] = state.getProperty("PASSWORD");
    args[4] = state.getString("seqTableName");
    args[5] = state.getInstance().getInstanceName();
    args[6] = state.getProperty("ZOOKEEPERS");
    args[7] = args[4] + "_MR";

    if (ToolRunner.run(CachedConfiguration.getInstance(), new MapRedVerifyTool(), args) != 0) {
      log.error("Failed to run map/red verify");
      return;
    }

    Scanner outputScanner = state.getConnector().createScanner(args[7], Authorizations.EMPTY);
    outputScanner.setRange(new Range());

    int count = 0;
    Key lastKey = null;
    for (Entry<Key,Value> entry : outputScanner) {
      Key current = entry.getKey();
      if (lastKey != null && lastKey.getColumnFamily().equals(current.getRow())) {
        log.info(entry.getKey());
        count++;
      }
      lastKey = current;
    }

    if (count > 1) {
      log.error("Gaps in output");
    }

    log.debug("Dropping table: " + args[7]);
    Connector conn = state.getConnector();
    conn.tableOperations().delete(args[7]);
  }
}
