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
package org.apache.accumulo.server.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.Test;

public class ServerMutationTest {

  @Test
  public void test() throws Exception {
    ServerMutation m = new ServerMutation(new Text("r1"));
    m.put(new Text("cf1"), new Text("cq1"), new Value("v1"));
    m.put(new Text("cf2"), new Text("cq2"), 56, new Value("v2"));
    m.setSystemTimestamp(42);

    List<ColumnUpdate> updates = m.getUpdates();

    assertEquals(2, updates.size());

    assertEquals("r1", new String(m.getRow()));
    ColumnUpdate cu = updates.get(0);

    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertEquals("", new String(cu.getColumnVisibility()));
    assertFalse(cu.hasTimestamp());
    assertEquals(42L, cu.getTimestamp());

    ServerMutation m2 = new ServerMutation();
    ReflectionUtils.copy(new Configuration(), m, m2);

    updates = m2.getUpdates();

    assertEquals(2, updates.size());
    assertEquals("r1", new String(m2.getRow()));

    cu = updates.get(0);
    assertEquals("cf1", new String(cu.getColumnFamily()));
    assertEquals("cq1", new String(cu.getColumnQualifier()));
    assertFalse(cu.hasTimestamp());
    assertEquals(42L, cu.getTimestamp());

    cu = updates.get(1);

    assertEquals("r1", new String(m2.getRow()));
    assertEquals("cf2", new String(cu.getColumnFamily()));
    assertEquals("cq2", new String(cu.getColumnQualifier()));
    assertTrue(cu.hasTimestamp());
    assertEquals(56, cu.getTimestamp());

  }

}
