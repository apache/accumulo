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
package org.apache.accumulo.cluster.standalone;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class StandaloneClusterControlTest {

  @Test
  public void testPaths() {
    String accumuloHome = "/usr/lib/accumulo", accumuloConfDir = "/etc/accumulo/conf",
        accumuloServerConfDir = "/etc/accumulo/conf/server";

    StandaloneClusterControl control =
        new StandaloneClusterControl(accumuloHome, accumuloConfDir, accumuloServerConfDir, "", "");

    assertEquals(accumuloHome, control.accumuloHome);
    assertEquals(accumuloConfDir, control.clientAccumuloConfDir);
    assertEquals(accumuloServerConfDir, control.serverAccumuloConfDir);

    assertEquals(accumuloHome + "/bin/accumulo", control.accumuloPath);
    assertEquals(accumuloHome + "/bin/accumulo-service", control.accumuloServicePath);
  }
}
