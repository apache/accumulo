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
package org.apache.accumulo.server.replication;

import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

/**
 * We don't want to introduce an upgrade path to 1.7 only for some new nodes within ZooKeeper
 * <p>
 * We can take the penalty of embedding this logic into the server processes, but alleviate users/developers from having to worry about the zookeeper state.
 */
public class ZooKeeperInitialization {
  /**
   * Ensure that the full path to ZooKeeper nodes that will be used exist
   */
  public static void ensureZooKeeperInitialized(final ZooReaderWriter zooReaderWriter, final String zRoot) throws KeeperException, InterruptedException {
    if (!zooReaderWriter.exists(zRoot + ReplicationConstants.ZOO_TSERVERS, null)) {
      zooReaderWriter.mkdirs(zRoot + ReplicationConstants.ZOO_TSERVERS);
    }

    if (!zooReaderWriter.exists(zRoot + ReplicationConstants.ZOO_WORK_QUEUE, null)) {
      zooReaderWriter.mkdirs(zRoot + ReplicationConstants.ZOO_WORK_QUEUE);
    }
  }
}
