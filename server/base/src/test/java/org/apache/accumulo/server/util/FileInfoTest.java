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
package org.apache.accumulo.server.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.server.util.FileUtil.FileInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FileInfoTest {
  private Key key1;
  private Key key2;
  private FileInfo info;

  @BeforeEach
  public void setUp() {
    key1 = new Key("row1");
    key2 = new Key("row2");
    info = new FileInfo(key1, key2);
  }

  @Test
  public void testGetters() {
    assertEquals("row1", info.getFirstRow().toString());
    assertEquals("row2", info.getLastRow().toString());
  }
}
