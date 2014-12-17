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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class IsolatedScan extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();
    
    Random rand = (Random) state.get("rand");
    
    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");
    
    String tableName = tableNames.get(rand.nextInt(tableNames.size()));
    
    try {
      RowIterator iter = new RowIterator(new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY)));
      
      while (iter.hasNext()) {
        PeekingIterator<Entry<Key,Value>> row = new PeekingIterator<Entry<Key,Value>>(iter.next());
        Entry<Key,Value> kv = null;
        if (row.hasNext())
          kv = row.peek();
        while (row.hasNext()) {
          Entry<Key,Value> currentKV = row.next();
          if (!kv.getValue().equals(currentKV.getValue()))
            throw new Exception("values not equal " + kv + " " + currentKV);
        }
      }
      log.debug("Isolated scan " + tableName);
    } catch (TableDeletedException e) {
      log.debug("Isolated scan " + tableName + " failed, table deleted");
    } catch (TableNotFoundException e) {
      log.debug("Isolated scan " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException e) {
      log.debug("Isolated scan " + tableName + " failed, offline");
    }
  }
}
