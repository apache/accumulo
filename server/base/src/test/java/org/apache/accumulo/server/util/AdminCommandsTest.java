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
package org.apache.accumulo.server.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class AdminCommandsTest {
  @Test
  public void testStopCommand() {
    Admin.StopCommand cmd = new Admin.StopCommand();
    assertEquals(0, cmd.args.size());
  }

  @Test
  public void testPingCommand() {
    Admin.PingCommand cmd = new Admin.PingCommand();
    assertEquals(0, cmd.args.size());
  }

  @Test
  public void testCheckTabletsCommand() {
    Admin.CheckTabletsCommand cmd = new Admin.CheckTabletsCommand();
    assertFalse(cmd.fixFiles);
    assertNull(cmd.table);
  }

  @Test
  public void testStopMasterCommand() {
    new Admin.StopMasterCommand();
  }

  @Test
  public void testStopAllCommand() {
    new Admin.StopAllCommand();
  }

  @Test
  public void testListInstancesCommand() {
    Admin.ListInstancesCommand cmd = new Admin.ListInstancesCommand();
    assertFalse(cmd.printErrors);
    assertFalse(cmd.printAll);
  }

  @Test
  public void testVolumesCommand() {
    Admin.VolumesCommand cmd = new Admin.VolumesCommand();
    assertFalse(cmd.printErrors);
  }

  @Test
  public void testDumpConfigCommand() {
    Admin.DumpConfigCommand cmd = new Admin.DumpConfigCommand();
    assertEquals(0, cmd.tables.size());
    assertFalse(cmd.allConfiguration);
    assertFalse(cmd.systemConfiguration);
    assertFalse(cmd.namespaceConfiguration);
    assertFalse(cmd.users);
    assertNull(cmd.directory);
  }

  // not a command, but easy enough to include here
  @Test
  public void testAdminOpts() {
    Admin.AdminOpts opts = new Admin.AdminOpts();
    assertFalse(opts.force);
  }
}
