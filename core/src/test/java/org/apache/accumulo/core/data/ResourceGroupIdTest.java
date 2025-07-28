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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

public class ResourceGroupIdTest {
  @Test
  public void testIllegalIds() {
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of(""));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of(" "));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("\t"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("_"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("9"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("9group1"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("$"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("$group1"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("group1 "));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("group 1"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("gro$up1"));
    assertThrows(IllegalArgumentException.class, () -> ResourceGroupId.of("invalid_Group_"));
  }

  @Test
  public void testGroupNamePattern() {
    ResourceGroupId.validateGroupNames(List.of("a"));
    ResourceGroupId.validateGroupNames(List.of("a", "b"));
    ResourceGroupId.validateGroupNames(List.of("default", "reg_ular"));
    assertThrows(RuntimeException.class,
        () -> ResourceGroupId.validateGroupNames(List.of("a1b2c3d4__")));
    assertThrows(RuntimeException.class,
        () -> ResourceGroupId.validateGroupNames(List.of("0abcde")));
    assertThrows(RuntimeException.class, () -> ResourceGroupId.validateGroupNames(List.of("a-b")));
    assertThrows(RuntimeException.class, () -> ResourceGroupId.validateGroupNames(List.of("a*b")));
    assertThrows(RuntimeException.class, () -> ResourceGroupId.validateGroupNames(List.of("a?b")));
  }

}
