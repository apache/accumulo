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

package org.apache.accumulo.core.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.CurrentLogsSection;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MetadataTableSchemaTest {

  @Test
  public void testGetTabletServer() throws Exception {
    Key key = new Key("~wal+host:43861[14a7df0e6420003]", "log", "hdfs://localhost:50514/accumulo/wal/host:43861/70c27ab3-6662-40ab-80fb-01c1f1a59df3");
    Text hostPort = new Text();
    Text session = new Text();
    CurrentLogsSection.getTabletServer(key, hostPort, session);
    assertEquals("host:43861", hostPort.toString());
    assertEquals("14a7df0e6420003", session.toString());
    try {
      Key bogus = new Key("~wal/host:43861[14a7df0e6420003]", "log", "hdfs://localhost:50514/accumulo/wal/host:43861/70c27ab3-6662-40ab-80fb-01c1f1a59df3");
      CurrentLogsSection.getTabletServer(bogus, hostPort, session);
      fail("bad argument not thrown");
    } catch (IllegalArgumentException ex) {

    }
  }

}
