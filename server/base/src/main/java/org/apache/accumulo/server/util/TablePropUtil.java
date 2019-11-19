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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.zookeeper.ZooCache.TABLE_SETTING_CONFIG_PATTERN;

import java.util.regex.Matcher;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablePropUtil {

  private static final Logger log = LoggerFactory.getLogger(TablePropUtil.class);

  public static boolean setTableProperty(ServerContext context, TableId tableId, String property,
      String value) throws KeeperException, InterruptedException {
    return setTableProperty(context.getZooReaderWriter(), context.getZooKeeperRoot(), tableId,
        property, value);
  }

  public static boolean setTableProperty(ZooReaderWriter zoo, String zkRoot, TableId tableId,
      String property, String value) throws KeeperException, InterruptedException {
    if (!isPropertyValid(property, value))
      return false;

    // create the zk node for per-table properties for this table if it doesn't already exist
    String zkTablePath = getTablePath(zkRoot, tableId);
    zoo.putPersistentData(zkTablePath, new byte[0], NodeExistsPolicy.SKIP);

    // create the zk node for this property and set it's data to the specified value
    String zPath = zkTablePath + "/" + property;
    zoo.putPersistentData(zPath, value.getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    updateTableConfigTrackingZnode(zPath, zoo);
    if (log.isTraceEnabled()) {
      log.trace("updateTableConfigTrackingZnode called in setTableProperty for " + zPath);
    }
    return true;
  }

  public static boolean isPropertyValid(String property, String value) {
    Property p = Property.getPropertyByKey(property);
    return (p == null || p.getType().isValidFormat(value))
        && Property.isValidTablePropertyKey(property);
  }

  public static void removeTableProperty(ServerContext context, TableId tableId, String property)
      throws InterruptedException, KeeperException {
    String zPath = getTablePath(context.getZooKeeperRoot(), tableId) + "/" + property;
    context.getZooReaderWriter().recursiveDelete(zPath, NodeMissingPolicy.SKIP);
    updateTableConfigTrackingZnode(zPath, context.getZooReaderWriter());
    if (log.isTraceEnabled()) {
      log.trace("updateTableConfigTrackingZnode called in removeTableProperty " + zPath);
    }
  }

  private static String getTablePath(String zkRoot, TableId tableId) {
    return zkRoot + Constants.ZTABLES + "/" + tableId.canonical() + Constants.ZTABLE_CONF;
  }

  /**
   * Sets the table-config-version Zookeeper node value with the ZPath of the table configuration
   * item that has just been created, updated or deleted. This triggers the NodeDataChanged event
   * for the table-config-version which cause all the ZooCaches to be updated.
   *
   * @param zPath
   *          The Zookeeper path to the table configuration being modified
   * @param zoo
   *          The ZooReaderWriter that will update the table-config-version
   * @throws KeeperException
   *           If the putPersistant data call this exception may be thrown
   * @throws InterruptedException
   *           If the putPersistant data call this exception may be thrown
   */

  public static void updateTableConfigTrackingZnode(String zPath, ZooReaderWriter zoo)
      throws KeeperException, InterruptedException {

    String pathPrefix;
    Matcher configMatcher = TABLE_SETTING_CONFIG_PATTERN.matcher(zPath);
    if (configMatcher.matches()) {
      pathPrefix = configMatcher.group(1);
      zoo.putPersistentData(pathPrefix + Constants.ZTABLE_CONFIG_VERSION, zPath.getBytes(UTF_8),
          NodeExistsPolicy.OVERWRITE);

    }

  }

}
