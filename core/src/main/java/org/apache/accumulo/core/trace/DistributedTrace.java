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
package org.apache.accumulo.core.trace;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.zookeeper.KeeperException;

public class DistributedTrace {
  public static void enable(Instance instance, ZooReader zoo, String application, String address) throws IOException, KeeperException, InterruptedException {
    String path = ZooUtil.getRoot(instance) + Constants.ZTRACERS;
    if (address == null) {
      try {
        address = InetAddress.getLocalHost().getHostAddress().toString();
      } catch (UnknownHostException e) {
        address = "unknown";
      }
    }
    Tracer.getInstance().addReceiver(new ZooTraceClient(zoo, path, address, application, 1000));
  }
}
