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
package org.apache.accumulo.master.upgrade;

import static org.apache.accumulo.core.Constants.BULK_PREFIX;
import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.junit.Test;

public class Upgrader9to10Test {
  @Test
  public void testSwitchRelative() {
    assertEquals(GcVolumeUtil.getDeleteTabletOnAllVolumesUri(TableId.of("5a"), "t-0005"),
        Upgrader9to10.switchToAllVolumes("/5a/t-0005"));
    assertEquals("/5a/" + BULK_PREFIX + "0005",
        Upgrader9to10.switchToAllVolumes("/5a/" + BULK_PREFIX + "0005"));
    assertEquals("/5a/t-0005/F0009.rf", Upgrader9to10.switchToAllVolumes("/5a/t-0005/F0009.rf"));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelativeTooShort() {
    Upgrader9to10.switchToAllVolumes("/5a");
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelativeTooLong() {
    Upgrader9to10.switchToAllVolumes("/5a/5a/t-0005/F0009.rf");
  }

  @Test
  public void testSwitch() {
    assertEquals(GcVolumeUtil.getDeleteTabletOnAllVolumesUri(TableId.of("5a"), "t-0005"),
        Upgrader9to10.switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/t-0005"));
    assertEquals("hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005", Upgrader9to10
        .switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/" + BULK_PREFIX + "0005"));
    assertEquals("hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf", Upgrader9to10
        .switchToAllVolumes("hdfs://localhost:9000/accumulo/tables/5a/t-0005/C0009.rf"));
  }

  @Test
  public void testUpgradeDir() {
    assertEquals("t-0005",
        Upgrader9to10.upgradeDirColumn("hdfs://localhost:9000/accumulo/tables/5a/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("../5a/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("/t-0005"));
    assertEquals("t-0005", Upgrader9to10.upgradeDirColumn("t-0005"));
  }
}
