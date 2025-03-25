/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager;

import java.util.function.BiConsumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.zookeeper.ZooCache.ZooCacheWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TableStateWatcher implements ZooCacheWatcher {

  private final BiConsumer<TableId,WatchedEvent> stateChangeConsumer;

  TableStateWatcher(BiConsumer<TableId,WatchedEvent> stateChangeConsumer) {
    this.stateChangeConsumer = stateChangeConsumer;
  }

  private static final Logger log = LoggerFactory.getLogger(TableStateWatcher.class);

  @Override
  public void accept(WatchedEvent event) {
    log.trace("{}", event);
    final String zPath = event.getPath();
    final EventType zType = event.getType();

    TableId tableId = null;

    if (zPath != null && zPath.startsWith(Constants.ZTABLES + "/")) {
      String suffix = zPath.substring(Constants.ZTABLES.length() + 1);
      if (suffix.contains("/")) {
        String[] sa = suffix.split("/", 2);
        if (Constants.ZTABLE_STATE.equals("/" + sa[1])) {
          tableId = TableId.of(sa[0]);
        }
      }
      if (tableId == null) {
        log.trace("Unhandled path {}", event);
        return;
      }
    }

    switch (zType) {
      case NodeCreated:
      case NodeDataChanged:
        // state transition
        if (tableId != null) {
          stateChangeConsumer.accept(tableId, event);
        }
        break;
      case NodeChildrenChanged:
        // According to documentation we should not receive this event now
        // that ZooCache is using Persistent Watchers. Not logging an error here.
        // According to https://issues.apache.org/jira/browse/ZOOKEEPER-4475 we
        // may receive this event (Fixed in 3.9.0)
      case NodeDeleted:
        // ignore
      case None:
        // ignore
        break;
      default:
        log.warn("Unhandled {}", event);
    }
  }
}
