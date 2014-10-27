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

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReader;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.zookeeper.KeeperException;

public class AccumuloStatus {
  /**
   * Determines if there could be an accumulo instance running via zookeeper lock checking
   *
   * @return true iff all servers show no indication of being registered in zookeeper, otherwise false
   * @throws IOException
   *           if there are issues connecting to ZooKeeper to determine service status
   */
  public static boolean isAccumuloOffline(IZooReader reader) throws IOException {
    String rootPath = ZooUtil.getRoot(HdfsZooInstance.getInstance());
    return isAccumuloOffline(reader, rootPath);
  }

  /**
   * Determines if there could be an accumulo instance running via zookeeper lock checking
   *
   * @return true iff all servers show no indication of being registered in zookeeper, otherwise false
   * @throws IOException
   *           if there are issues connecting to ZooKeeper to determine service status
   */
  public static boolean isAccumuloOffline(IZooReader reader, String rootPath) throws IOException {
    try {
      for (String child : reader.getChildren(rootPath + Constants.ZTSERVERS)) {
        if (!reader.getChildren(rootPath + Constants.ZTSERVERS + "/" + child).isEmpty())
          return false;
      }
      if (!reader.getChildren(rootPath + Constants.ZTRACERS).isEmpty())
        return false;
      if (!reader.getChildren(rootPath + Constants.ZMASTER_LOCK).isEmpty())
        return false;
      if (!reader.getChildren(rootPath + Constants.ZMONITOR_LOCK).isEmpty())
        return false;
      if (!reader.getChildren(rootPath + Constants.ZGC_LOCK).isEmpty())
        return false;
    } catch (KeeperException e) {
      throw new IOException("Issues contacting ZooKeeper to get Accumulo status.", e);
    } catch (InterruptedException e) {
      throw new IOException("Issues contacting ZooKeeper to get Accumulo status.", e);
    }
    return true;
  }

}
