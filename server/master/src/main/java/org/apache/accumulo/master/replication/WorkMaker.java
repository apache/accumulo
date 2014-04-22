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
package org.apache.accumulo.master.replication;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.log4j.Logger;

/**
 * 
 */
public class WorkMaker extends Daemon {
  private static final Logger log = Logger.getLogger(WorkMaker.class);

  final Connector conn;
  final AccumuloConfiguration conf;

  public WorkMaker(Connector conn, AccumuloConfiguration conf) {
    super("Replication Table Work Maker");
    this.conn = conn;
    this.conf = conf;
  }

  @Override
  public void run() {
    while (true) {
      if (!conn.tableOperations().exists(ReplicationTable.NAME)) {
        log.trace("Replication table does not yet exist");
        UtilWaitThread.sleep(5000);
      }

      final Scanner s;
      try {
        s = ReplicationTable.getScanner(conn);
      } catch (TableNotFoundException e) {
        log.warn("Replication table was deleted");
        continue;
      }

      s.setRange(ReplicationSchema.ReplicationSection.getRange());
      TableConfiguration tableConf;

      for (Entry<Key,Value> entry : s) {
        String tableId = ReplicationSchema.ReplicationSection.getTableId(entry.getKey()); 
        tableConf = ServerConfiguration.getTableConfiguration(conn.getInstance(), tableId);

        Map<String,String> replicationTargets = tableConf.getAllPropertiesWithPrefix(Property.TABLE_REPLICATION_TARGETS);
        addWorkRecord(entry.getKey(), replicationTargets);
      }
    }
  }

  protected void addWorkRecord(Key k, Map<String,String> targets) {
    
  }
}
