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
package org.apache.accumulo.core.fate.zookeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ZooSessionTest {

  private static final int TIMEOUT_SECONDS = 10;
  private static final String UNKNOWN_HOST = "hostname.that.should.not.exist.example.com:2181";

  @Test
  @Timeout(TIMEOUT_SECONDS * 4)
  public void testUnknownHost() {
    assertThrows(RuntimeException.class, () -> {
      ZooKeeper session = ZooSession.connect(UNKNOWN_HOST, (int) SECONDS.toMillis(TIMEOUT_SECONDS),
          null, null, null);
      session.close();
    });
  }

}
