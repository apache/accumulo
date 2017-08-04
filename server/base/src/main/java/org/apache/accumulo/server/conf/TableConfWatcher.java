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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

class TableConfWatcher implements Watcher {
  static {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    Logger.getLogger("org.apache.hadoop.io.compress").setLevel(Level.WARN);
  }

  private static final Logger log = Logger.getLogger(TableConfWatcher.class);
  private final Instance instance;
  private final String tablesPrefix;
  private ServerConfigurationFactory scf;

  TableConfWatcher(Instance instance) {
    this.instance = instance;
    tablesPrefix = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/";
    scf = new ServerConfigurationFactory(instance);
  }

  static String toString(WatchedEvent event) {
    return new StringBuilder("{path=").append(event.getPath()).append(",state=").append(event.getState()).append(",type=").append(event.getType()).append("}")
        .toString();
  }

  @Override
  public void process(WatchedEvent event) {
    String path = event.getPath();
    if (log.isTraceEnabled())
      log.trace("WatchedEvent : " + toString(event));

    String tableIdString = null;
    String key = null;

    if (path != null) {
      if (path.startsWith(tablesPrefix)) {
        tableIdString = path.substring(tablesPrefix.length());
        if (tableIdString.contains("/")) {
          tableIdString = tableIdString.substring(0, tableIdString.indexOf('/'));
          if (path.startsWith(tablesPrefix + tableIdString + Constants.ZTABLE_CONF + "/"))
            key = path.substring((tablesPrefix + tableIdString + Constants.ZTABLE_CONF + "/").length());
        }
      }

      if (tableIdString == null) {
        log.warn("Zookeeper told me about a path I was not watching: " + path + ", event " + toString(event));
        return;
      }
    }
    Table.ID tableId = Table.ID.of(tableIdString);

    switch (event.getType()) {
      case NodeDataChanged:
        if (log.isTraceEnabled())
          log.trace("EventNodeDataChanged " + event.getPath());
        if (key != null)
          scf.getTableConfiguration(tableId).propertyChanged(key);
        break;
      case NodeChildrenChanged:
        scf.getTableConfiguration(tableId).propertiesChanged();
        break;
      case NodeDeleted:
        if (key == null) {
          // only remove the AccumuloConfiguration object when a
          // table node is deleted, not when a tables property is
          // deleted.
          ServerConfigurationFactory.removeCachedTableConfiguration(instance.getInstanceID(), tableId);
        }
        break;
      case None:
        switch (event.getState()) {
          case Expired:
            ServerConfigurationFactory.expireAllTableObservers();
            break;
          case SyncConnected:
            break;
          case Disconnected:
            break;
          default:
            log.warn("EventNone event not handled " + toString(event));
        }
        break;
      case NodeCreated:
        switch (event.getState()) {
          case SyncConnected:
            break;
          default:
            log.warn("Event NodeCreated event not handled " + toString(event));
        }
        break;
      default:
        log.warn("Event not handled " + toString(event));
    }
  }
}
