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
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

class TableNamespaceConfWatcher implements Watcher {
  static {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    Logger.getLogger("org.apache.hadoop.io.compress").setLevel(Level.WARN);
  }
  
  private static final Logger log = Logger.getLogger(TableNamespaceConfWatcher.class);
  private Instance instance = null;
  
  TableNamespaceConfWatcher(Instance instance) {
    this.instance = instance;
  }
  
  @Override
  public void process(WatchedEvent event) {
    String path = event.getPath();
    if (log.isTraceEnabled())
      log.trace("WatchEvent : " + path + " " + event.getState() + " " + event.getType());
    
    String namespacesPrefix = ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/";
    
    String namespaceId = null;
    String key = null;
    
    if (path != null) {
      if (path.startsWith(namespacesPrefix)) {
        namespaceId = path.substring(namespacesPrefix.length());
        if (namespaceId.contains("/")) {
          namespaceId = namespaceId.substring(0, namespaceId.indexOf('/'));
          if (path.startsWith(namespacesPrefix + namespaceId + Constants.ZNAMESPACE_CONF + "/"))
            key = path.substring((namespacesPrefix + namespaceId + Constants.ZNAMESPACE_CONF + "/").length());
        }
      }
      
      if (namespaceId == null) {
        log.warn("Zookeeper told me about a path I was not watching " + path + " state=" + event.getState() + " type=" + event.getType());
        return;
      }
    }
    
    switch (event.getType()) {
      case NodeDataChanged:
        if (log.isTraceEnabled())
          log.trace("EventNodeDataChanged " + event.getPath());
        if (key != null)
          ServerConfiguration.getTableNamespaceConfiguration(instance, namespaceId).propertyChanged(key);
        break;
      case NodeChildrenChanged:
        ServerConfiguration.getTableNamespaceConfiguration(instance, namespaceId).propertiesChanged(key);
        break;
      case NodeDeleted:
        if (key == null) {
          ServerConfiguration.removeNamespaceIdInstance(namespaceId);
        }
        break;
      case None:
        switch (event.getState()) {
          case Expired:
            ServerConfiguration.expireAllTableObservers();
            break;
          case SyncConnected:
            break;
          case Disconnected:
            break;
          default:
            log.warn("EventNone event not handled path = " + event.getPath() + " state=" + event.getState());
        }
        break;
      case NodeCreated:
        switch (event.getState()) {
          case SyncConnected:
            break;
          default:
            log.warn("Event NodeCreated event not handled path = " + event.getPath() + " state=" + event.getState());
        }
        break;
      default:
        log.warn("Event not handled path = " + event.getPath() + " state=" + event.getState() + " type = " + event.getType());
    }
  }
}
