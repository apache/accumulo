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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class BatchScan extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    Random rand = (Random) state.get("rand");

    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");

    String tableName = tableNames.get(rand.nextInt(tableNames.size()));

    try {
      BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 3);
      List<Range> ranges = new ArrayList<Range>();
      for (int i = 0; i < rand.nextInt(2000) + 1; i++)
        ranges.add(new Range(String.format("%016x", rand.nextLong() & 0x7fffffffffffffffl)));

      bs.setRanges(ranges);

      try {
        Iterator<Entry<Key,Value>> iter = bs.iterator();
        while (iter.hasNext())
          iter.next();
      } finally {
        bs.close();
      }

      log.debug("Wrote to " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("BatchScan " + tableName + " failed, doesnt exist");
    } catch (TableDeletedException tde) {
      log.debug("BatchScan " + tableName + " failed, table deleted");
    } catch (TableOfflineException e) {
      log.debug("BatchScan " + tableName + " failed, offline");
    } catch (RuntimeException e) {
      if (e.getCause() instanceof AccumuloSecurityException) {
        log.debug("BatchScan " + tableName + " failed, permission error");
      } else {
        throw e;
      }
    }
  }
}
