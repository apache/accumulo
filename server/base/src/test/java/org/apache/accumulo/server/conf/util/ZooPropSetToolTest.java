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

package org.apache.accumulo.server.conf.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ZooPropSetToolTest {

  @Test
  public void optionsAllDefault() {
    ZooPropSetTool.Opts opts = new ZooPropSetTool.Opts();
    assertTrue(opts.setOpt.isEmpty());
    assertTrue(opts.deleteOpt.isEmpty());
  }

  @Test
  public void invalidSetAndDelete() {
    ZooPropSetTool.Opts opts = new ZooPropSetTool.Opts();
    assertThrows(IllegalArgumentException.class, () -> opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"-s", "foo=1", "-d", "bar=2"}));
  }

  @Test
  public void failSetAndFilter() {
    ZooPropSetTool.Opts opts = new ZooPropSetTool.Opts();
    assertThrows(IllegalArgumentException.class, () -> opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"-s", "foo=1", "-f", "bloom"}));
  }

  @Test
  public void failDeleteAndFilter() {
    ZooPropSetTool.Opts opts = new ZooPropSetTool.Opts();
    assertThrows(IllegalArgumentException.class, () -> opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"-d", "foo", "-f", "bloom"}));
  }
}
