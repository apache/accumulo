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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.google.common.annotations.VisibleForTesting;

public class MonitorUtil {
  public static String getLocation(Instance instance) throws KeeperException, InterruptedException {
    return getLocation(new ZooReader(instance.getZooKeepers(), 30000), instance);
  }

  @VisibleForTesting
  static String getLocation(ZooReader zr, Instance instance)
      throws KeeperException, InterruptedException {
    try {
      byte[] loc = zr.getData(ZooUtil.getRoot(instance) + Constants.ZMONITOR_HTTP_ADDR, null);
      return loc == null ? null : new String(loc, UTF_8);
    } catch (NoNodeException e) {
      // If there's no node advertising the monitor, there's no monitor.
      return null;
    }
  }
}
