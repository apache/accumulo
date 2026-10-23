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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ScanServerRefTabletFileTest {

  @Test
  public void tableIdsRoundTripForExternalPaths() {
    UUID serverLockUuid = UUID.randomUUID();
    String serverAddress = "localhost:1234";
    String file = "hdfs://nn/external/rfiles/source.rf";
    Set<TableId> tableIds = Set.of(TableId.of("2a"), TableId.of("3b"));
    var reference = new ScanServerRefTabletFile(serverLockUuid, serverAddress, file, tableIds);
    var key = new Key(new Text(serverLockUuid.toString()), new Text(serverAddress),
        reference.getFilePath(), new Text(), 1);

    var decoded = new ScanServerRefTabletFile(key, reference.getValue());

    assertEquals(file, decoded.getNormalizedPathStr());
    assertEquals(tableIds, decoded.getTableIds());
  }

  @Test
  public void tableIdsAreRequired() {
    String serverAddress = "localhost:1234";
    String file = "hdfs://nn/external/rfiles/source.rf";

    assertThrows(NullPointerException.class,
        () -> new ScanServerRefTabletFile(UUID.randomUUID(), serverAddress, file, null));
    assertThrows(IllegalArgumentException.class,
        () -> new ScanServerRefTabletFile(UUID.randomUUID(), serverAddress, file, Set.of()));

    var key = new Key(new Text(UUID.randomUUID().toString()), new Text(serverAddress),
        new Text(file), new Text(), 1);
    assertThrows(IllegalArgumentException.class,
        () -> new ScanServerRefTabletFile(key, new Value()));
  }
}
