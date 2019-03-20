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
package org.apache.accumulo.master.tableOps.tableImport;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.easymock.EasyMock;
import org.junit.Test;

public class ImportTableTest {

  @Test
  public void testTabletDir() {
    Master master = EasyMock.createMock(Master.class);
    VolumeManager volumeManager = EasyMock.createMock(VolumeManager.class);
    ImportedTableInfo iti = new ImportedTableInfo();
    iti.tableId = TableId.of("5");

    // Different volumes with different paths
    String[] volumes = {"hdfs://nn1:8020/apps/accumulo1", "hdfs://nn2:8020/applications/accumulo"};
    // This needs to be unique WRT the importtable command
    String tabletDir = "/c-00000001";

    EasyMock.expect(master.getContext()).andReturn(null);
    EasyMock.expect(master.getFileSystem()).andReturn(volumeManager);
    // Choose the 2nd element
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(iti.tableId, null, null);
    EasyMock.expect(volumeManager.choose(EasyMock.eq(chooserEnv), EasyMock.eq(volumes)))
        .andReturn(volumes[1]);

    EasyMock.replay(master, volumeManager);

    PopulateMetadataTable pmt = new PopulateMetadataTable(iti);
    assertEquals(volumes[1] + "/" + ServerConstants.TABLE_DIR + "/" + iti.tableId + "/" + tabletDir,
        pmt.getClonedTabletDir(master, null, volumes, tabletDir));

    EasyMock.verify(master, volumeManager);
  }

}
