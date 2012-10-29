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
package org.apache.accumulo.server.util;

import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.Listener;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.log4j.Logger;

public class FindOfflineTablets {
  private static final Logger log = Logger.getLogger(FindOfflineTablets.class);
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: accumulo.server.util.FindOfflineTablets instance zookeepers");
      System.exit(1);
    }
    String instance = args[0];
    String keepers = args[1];
    Instance zooInst = new ZooKeeperInstance(instance, keepers);
    MetaDataTableScanner scanner = new MetaDataTableScanner(zooInst, SecurityConstants.getSystemCredentials(), new Range());
    LiveTServerSet tservers = new LiveTServerSet(zooInst, DefaultConfiguration.getDefaultConfiguration(), new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
        if (!deleted.isEmpty())
          log.warn("Tablet servers deleted while scanning: " + deleted);
        if (!added.isEmpty())
          log.warn("Tablet servers added while scanning: " + added);
      }
    });
    while (scanner.hasNext()) {
      TabletLocationState locationState = scanner.next();
      TabletState state = locationState.getState(tservers.getCurrentServers());
      if (state != TabletState.HOSTED && TableManager.getInstance().getTableState(locationState.extent.getTableId().toString()) != TableState.OFFLINE)
        System.out.println(locationState + " is " + state);
    }
  }
  
}
