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
package org.apache.accumulo.server.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.Test;

class LogSortInformationTest {

  @Test
  void roundTrip() {
    UUID sortId = UUID.randomUUID();
    LogSortInformation work = new LogSortInformation(sortId, DfsWalProvider.PROVIDER_ID,
        "hdfs://nn/wal/" + sortId, "hdfs://nn/recovery/sorted");

    assertEquals(work, LogSortInformation.deserialize(work.serialize()));
  }

  @Test
  void malformedPayload() {
    String id = UUID.randomUUID().toString();
    for (String payload : new String[] {id + "||source|dest", id + "|bad-id|source|dest",
        id + "|dfs||dest", id + "|dfs|source| "}) {
      assertThrows(IllegalArgumentException.class, () -> LogSortInformation.deserialize(payload));
    }
    assertThrows(IllegalArgumentException.class,
        () -> new LogSortInformation(UUID.randomUUID(), "dfs", "source|extra", "dest"));
    assertThrows(IllegalArgumentException.class,
        () -> LogSortInformation.deserialize("source|dest"));
    assertThrows(IllegalArgumentException.class,
        () -> LogSortInformation.deserialize("not-a-uuid|source|dest"));
    assertThrows(IllegalArgumentException.class,
        () -> LogSortInformation.deserialize(UUID.randomUUID() + "|source|dest"));
    assertThrows(IllegalArgumentException.class,
        () -> LogSortInformation.deserialize(UUID.randomUUID() + "provider|source|dest|extra"));
  }
}
