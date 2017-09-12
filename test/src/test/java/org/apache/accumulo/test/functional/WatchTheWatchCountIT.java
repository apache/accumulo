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
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;

// ACCUMULO-2757 - make sure we don't make too many more watchers
public class WatchTheWatchCountIT extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(WatchTheWatchCountIT.class);

  public int defaultOverrideSeconds() {
    return 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(3);
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String[] tableNames = getUniqueNames(3);
    for (String tableName : tableNames) {
      c.tableOperations().create(tableName);
    }
    c.tableOperations().list();
    String zooKeepers = c.getInstance().getZooKeepers();
    final Range<Long> expectedWatcherRange = Range.open(475l, 700l);
    long total = 0;
    final HostAndPort hostAndPort = HostAndPort.fromString(zooKeepers);
    for (int i = 0; i < 5; i++) {
      Socket socket = new Socket(hostAndPort.getHost(), hostAndPort.getPort());
      try {
        socket.getOutputStream().write("wchs\n".getBytes(), 0, 5);
        byte[] buffer = new byte[1024];
        int n = socket.getInputStream().read(buffer);
        String response = new String(buffer, 0, n);
        total = Long.parseLong(response.split(":")[1].trim());
        log.info("Total: {}", total);
        if (expectedWatcherRange.contains(total)) {
          break;
        }
        log.debug("Expected number of watchers to be contained in {}, but actually was {}. Sleeping and retrying", expectedWatcherRange, total);
        Thread.sleep(5000);
      } finally {
        socket.close();
      }
    }

    assertTrue("Expected number of watchers to be contained in " + expectedWatcherRange + ", but actually was " + total, expectedWatcherRange.contains(total));
  }

}
