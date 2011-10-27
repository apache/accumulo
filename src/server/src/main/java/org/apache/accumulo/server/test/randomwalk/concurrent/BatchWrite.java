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
package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class BatchWrite extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();
    
    Random rand = (Random) state.get("rand");
    
    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");
    
    String tableName = tableNames.get(rand.nextInt(tableNames.size()));
    
    try {
      BatchWriter bw = conn.createBatchWriter(tableName, 1000000, 60000l, 3);
      try {
        int numRows = rand.nextInt(100000);
        for (int i = 0; i < numRows; i++) {
          Mutation m = new Mutation(String.format("%016x", Math.abs(rand.nextLong())));
          long val = Math.abs(rand.nextLong());
          for (int j = 0; j < 10; j++) {
            m.put("cf", "cq" + j, new Value(String.format("%016x", val).getBytes()));
          }
          
          bw.addMutation(m);
        }
      } finally {
        bw.close();
      }
      
      log.debug("Wrote to " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("BatchWrite " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException e) {
      log.debug("BatchWrite " + tableName + " failed, offline");
    } catch (MutationsRejectedException mre) {
      if (mre.getCause() instanceof TableDeletedException)
        log.debug("BatchWrite " + tableName + " failed, table deleted");
      else if (mre.getCause() instanceof TableOfflineException)
        log.debug("BatchWrite " + tableName + " failed, offline");
      else
        throw mre;
    }
  }
}
