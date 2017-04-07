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

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.Fixture;
import org.apache.accumulo.test.randomwalk.State;

public class MultiTableFixture extends Fixture {

  @Override
  public void setUp(State state, Environment env) throws Exception {

    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");

    state.set("tableNamePrefix", String.format("multi_%s_%s_%d", hostname, env.getPid(), System.currentTimeMillis()));
    state.set("nextId", Integer.valueOf(0));
    state.set("numWrites", Long.valueOf(0));
    state.set("totalWrites", Long.valueOf(0));
    state.set("tableList", new CopyOnWriteArrayList<String>());
  }

  @Override
  public void tearDown(State state, Environment env) throws Exception {
    // We have resources we need to clean up
    if (env.isMultiTableBatchWriterInitialized()) {
      MultiTableBatchWriter mtbw = env.getMultiTableBatchWriter();
      try {
        mtbw.close();
      } catch (MutationsRejectedException e) {
        log.error("Ignoring mutations that weren't flushed", e);
      }

      // Reset the MTBW on the state to null
      env.resetMultiTableBatchWriter();
    }

    Connector conn = env.getConnector();

    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");

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
