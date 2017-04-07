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
package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletServerLockChecker;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.zookeeper.KeeperException;

/**
 *
 */
public class ZookeeperLockChecker implements TabletServerLockChecker {

  private final ZooCache zc;
  private final String root;

  ZookeeperLockChecker(Instance instance) {
    this(instance, new ZooCacheFactory());
  }

  ZookeeperLockChecker(Instance instance, ZooCacheFactory zcf) {
    zc = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    this.root = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
  }

  @Override
  public boolean isLockHeld(String tserver, String session) {
    try {
      return ZooLock.getSessionId(zc, root + "/" + tserver) == Long.parseLong(session, 16);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void invalidateCache(String tserver) {
    zc.clear(root + "/" + tserver);
  }

}
