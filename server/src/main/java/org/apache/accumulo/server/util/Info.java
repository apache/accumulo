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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

public class Info {
  public static void main(String[] args) throws Exception {
    ZooReaderWriter zrw = ZooReaderWriter.getInstance();
    Instance instance = HdfsZooInstance.getInstance();
    System.out.println("monitor: " + new String(zrw.getData(ZooUtil.getRoot(instance) + Constants.ZMONITOR_HTTP_ADDR, null), Constants.UTF8));
    System.out.println("masters: " + instance.getMasterLocations());
    System.out.println("zookeepers: " + instance.getZooKeepers());
  }
}
