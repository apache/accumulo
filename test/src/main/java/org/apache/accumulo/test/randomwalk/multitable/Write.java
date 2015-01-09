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

import static com.google.common.base.Charsets.UTF_8;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Write extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    @SuppressWarnings("unchecked")
    ArrayList<String> tables = (ArrayList<String>) state.get("tableList");

    if (tables.isEmpty()) {
      log.debug("No tables to ingest into");
      return;
    }

    Random rand = new Random();
    String tableName = tables.get(rand.nextInt(tables.size()));

    BatchWriter bw = null;
    try {
      bw = state.getMultiTableBatchWriter().getBatchWriter(tableName);
    } catch (TableOfflineException e) {
      log.error("Table " + tableName + " is offline!");
      return;
    } catch (TableNotFoundException e) {
      log.error("Table " + tableName + " not found!");
      return;
    }

    Text meta = new Text("meta");
    String uuid = UUID.randomUUID().toString();

    Mutation m = new Mutation(new Text(uuid));

    // create a fake payload between 4KB and 16KB
    int numBytes = rand.nextInt(12000) + 4000;
    byte[] payloadBytes = new byte[numBytes];
    rand.nextBytes(payloadBytes);
    m.put(meta, new Text("payload"), new Value(payloadBytes));

    // store size
    m.put(meta, new Text("size"), new Value(String.format("%d", numBytes).getBytes(UTF_8)));

    // store hash
    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(payloadBytes);
    m.put(meta, new Text("sha1"), new Value(alg.digest()));

    // add mutation
    bw.addMutation(m);

    state.set("numWrites", state.getLong("numWrites") + 1);
  }

}
