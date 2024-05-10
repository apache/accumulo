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
package org.apache.accumulo.server.manager.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class TabletManagementParametersTest {

  @Test
  public void testDeSer() {

    final ManagerState managerState = ManagerState.NORMAL;
    final Map<Ample.DataLevel,Boolean> parentUpgradeMap = Map.of(Ample.DataLevel.ROOT, true,
        Ample.DataLevel.USER, true, Ample.DataLevel.METADATA, true);
    final Set<TableId> onlineTables = Set.of(TableId.of("1"), TableId.of("2"), TableId.of("3"));
    final Set<TServerInstance> tservers = Set.of(new TServerInstance("127.0.0.1:10000", 0),
        new TServerInstance("127.0.0.1:10001", 1));
    final LiveTServerSet.LiveTServersSnapshot serverSnapshot =
        new LiveTServerSet.LiveTServersSnapshot(tservers,
            Map.of(Constants.DEFAULT_RESOURCE_GROUP_NAME, tservers));
    final Set<TServerInstance> serversToShutdown = Set.of();
    final Map<KeyExtent,TServerInstance> migrations = Map.of();
    final Ample.DataLevel dataLevel = Ample.DataLevel.USER;
    final Map<FateId,Map<String,String>> compactionHints = Map.of();
    final boolean canSuspendTablets = true;
    final Map<Path,Path> replacements =
        Map.of(new Path("file:/vol1/accumulo/inst_id"), new Path("file:/vol2/accumulo/inst_id"));
    final SteadyTime steadyTime = SteadyTime.from(100_000, TimeUnit.NANOSECONDS);

    final TabletManagementParameters tmp = new TabletManagementParameters(managerState,
        parentUpgradeMap, onlineTables, serverSnapshot, serversToShutdown, migrations, dataLevel,
        compactionHints, canSuspendTablets, replacements, steadyTime);

    String jsonString = tmp.serialize();
    TabletManagementParameters tmp2 = TabletManagementParameters.deserialize(jsonString);

    assertEquals(managerState, tmp2.getManagerState());
    assertEquals(parentUpgradeMap, tmp2.getParentUpgradeMap());
    assertEquals(onlineTables, tmp2.getOnlineTables());
    assertEquals(tservers, tmp2.getOnlineTsevers());
    assertEquals(serversToShutdown, tmp2.getServersToShutdown());
    assertEquals(migrations, tmp2.getMigrations());
    assertEquals(dataLevel, tmp2.getLevel());
    assertEquals(compactionHints, tmp2.getCompactionHints());
    assertEquals(canSuspendTablets, tmp2.canSuspendTablets());
    assertEquals(replacements, tmp2.getVolumeReplacements());
    assertEquals(steadyTime, tmp2.getSteadyTime());
  }
}
