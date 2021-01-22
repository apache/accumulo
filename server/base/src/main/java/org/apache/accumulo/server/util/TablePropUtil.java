/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

public class TablePropUtil {

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
  }

  private static String getTablePath(String zkRoot, TableId tableId) {
    return zkRoot + Constants.ZTABLES + "/" + tableId.canonical() + Constants.ZTABLE_CONF;
  }
}
