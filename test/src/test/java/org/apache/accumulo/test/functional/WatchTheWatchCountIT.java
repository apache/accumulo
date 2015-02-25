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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertTrue;

import java.net.Socket;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.net.HostAndPort;

// ACCUMULO-2757 - make sure we don't make too many more watchers
public class WatchTheWatchCountIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(3);
  }

  @Test(timeout = 30 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    for (String tableName : this.getUniqueNames(3)) {
      c.tableOperations().create(tableName);
    }
    c.tableOperations().list();
    String zooKeepers = c.getInstance().getZooKeepers();
    HostAndPort hostAndPort = HostAndPort.fromString(zooKeepers);
    Socket socket = new Socket(hostAndPort.getHostText(), hostAndPort.getPort());
    try {
      socket.getOutputStream().write("wchs\n".getBytes(), 0, 5);
      byte[] buffer = new byte[1024];
      int n = socket.getInputStream().read(buffer);
      String response = new String(buffer, 0, n);
      long total = Long.parseLong(response.split(":")[1].trim());
      assertTrue("Total watches was not greater than 600, but was " + total, total > 600);
      assertTrue("Total watches was not less than 600, but was " + total, total < 675);
    } finally {
      socket.close();
    }
  }

}
