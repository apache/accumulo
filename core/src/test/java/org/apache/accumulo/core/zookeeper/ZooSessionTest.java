/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.zookeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ZooSessionTest {

  private static final int ZK_TIMEOUT_SECONDS = 5;

  private String UNKNOWN_HOST = "hostname.that.should.not.exist.example.com:2181";
  private int millisTimeout = (int) SECONDS.toMillis(ZK_TIMEOUT_SECONDS);

  @Test
  @Timeout(ZK_TIMEOUT_SECONDS * 4)
  public void testConnectUnknownHost() {

    try (var zk = new ZooSession(getClass().getSimpleName(), UNKNOWN_HOST, millisTimeout, null)) {
      var e = assertThrows(IllegalStateException.class, () -> {
        zk.getSessionId();
      });
      assertTrue(e.getMessage().contains("Failed to connect to zookeeper (" + UNKNOWN_HOST
          + ") within 2x zookeeper timeout period " + millisTimeout));
    }
  }

  @Test
  public void testClosed() {
    ZooSession zk;
    try (var zk2 = zk = new ZooSession("testClosed", UNKNOWN_HOST, millisTimeout, null)) {
      assertNotNull(zk);
    }
    var e = assertThrows(IllegalStateException.class, () -> {
      zk.getSessionId();
    });
    assertTrue(e.getMessage().startsWith("ZooSession[testClosed_"), e.getMessage());
    assertTrue(e.getMessage().endsWith("] was closed"), e.getMessage());
  }

}
