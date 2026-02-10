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
package org.apache.accumulo.test.start;

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.accumulo.start.spi.UsageGroup;
import org.apache.accumulo.start.spi.UsageGroups;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SUNNY_DAY)
public class UsageGroupsIT {

  @Test
  public void testUsageGroups() {
    Set<UsageGroup> groups = UsageGroups.getUsageGroups();
    assertEquals(6, groups.size());
    assertTrue(groups.contains(UsageGroups.ADMIN));
    assertTrue(groups.contains(UsageGroups.CLIENT));
    assertTrue(groups.contains(UsageGroups.COMPACTION));
    assertTrue(groups.contains(UsageGroups.CORE));
    assertTrue(groups.contains(UsageGroups.OTHER));
    assertTrue(groups.contains(UsageGroups.PROCESS));
  }
}
