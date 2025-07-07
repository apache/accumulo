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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ColumnUpdateTest {
  /*
   * byte[] columnFamily = cf; byte[] columnQualifier = cq; byte[] columnVisibility = cv; boolean
   * hasTimestamp = hasts; long timestamp = ts; boolean deleted = deleted; byte[] val = val;
   */
  @Test
  void testHashCode() {
    // Testing consistency
    ColumnUpdate colUpdate = new ColumnUpdate("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8),
        "colv".getBytes(UTF_8), true, 1L, false, "val".getBytes(UTF_8));

    int hashCode1 = colUpdate.hashCode();
    int hashCode2 = colUpdate.hashCode();
    assertEquals(hashCode1, hashCode2);

    // Testing equality
    ColumnUpdate col1 = new ColumnUpdate("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8),
        "colv".getBytes(UTF_8), true, 1L, false, "val".getBytes(UTF_8));
    ColumnUpdate col2 = new ColumnUpdate("colfam".getBytes(UTF_8), "colq".getBytes(UTF_8),
        "colv".getBytes(UTF_8), true, 1L, false, "val".getBytes(UTF_8));
    assertEquals(col1.hashCode(), col2.hashCode());

    // Testing even distribution
    List<ColumnUpdate> columns = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      columns.add(new ColumnUpdate(("colfam" + i).getBytes(UTF_8), ("colq" + i).getBytes(UTF_8),
          ("colv" + i).getBytes(UTF_8), true, 1L, false, ("val" + i).getBytes(UTF_8)));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (ColumnUpdate c : columns) {
      hashCodes.add(c.hashCode());
    }
    assertEquals(columns.size(), hashCodes.size(), 10);

  }
}
