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
package org.apache.accumulo.manager.tableOps.split;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class UpdateTabletsTest {

  StoredTabletFile newSTF(int fileNum) {
    return new ReferencedTabletFile(new Path(
        "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F00000" + fileNum + ".rf"))
        .insert();
  }

  FileUtil.FileInfo newFileInfo(String start, String end) {
    return new FileUtil.FileInfo(new Text(start), new Text(end));
  }

  // When a tablet splits its files are partitioned among the new children tablets. This test
  // exercises the partitioning code.
  @Test
  public void testFileParitioning() {

    var file1 = newSTF(1);
    var file2 = newSTF(2);
    var file3 = newSTF(3);
    var file4 = newSTF(4);

    var tabletFiles =
        Map.of(file1, new DataFileValue(1000, 100, 20), file2, new DataFileValue(2000, 200, 50),
            file3, new DataFileValue(4000, 400), file4, new DataFileValue(4000, 400));

    var ke1 = new KeyExtent(TableId.of("1"), new Text("m"), null);
    var ke2 = new KeyExtent(TableId.of("1"), new Text("r"), new Text("m"));
    var ke3 = new KeyExtent(TableId.of("1"), new Text("v"), new Text("r"));
    var ke4 = new KeyExtent(TableId.of("1"), null, new Text("v"));

    var firstAndLastKeys = Map.of(file2, newFileInfo("m", "r"), file3, newFileInfo("g", "x"), file4,
        newFileInfo("s", "v"));

    var ke1Expected = Map.of(file1, new DataFileValue(250, 25, 20), file2,
        new DataFileValue(1000, 100, 50), file3, new DataFileValue(1000, 100));
    var ke2Expected = Map.of(file1, new DataFileValue(250, 25, 20), file2,
        new DataFileValue(1000, 100, 50), file3, new DataFileValue(1000, 100));
    var ke3Expected = Map.of(file1, new DataFileValue(250, 25, 20), file3,
        new DataFileValue(1000, 100), file4, new DataFileValue(4000, 400));
    var ke4Expected =
        Map.of(file1, new DataFileValue(250, 25, 20), file3, new DataFileValue(1000, 100));

    var expected = Map.of(ke1, ke1Expected, ke2, ke2Expected, ke3, ke3Expected, ke4, ke4Expected);

    Set<KeyExtent> newExtents = Set.of(ke1, ke2, ke3, ke4);

    TabletMetadata tabletMeta = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tabletMeta.getFilesMap()).andReturn(tabletFiles).anyTimes();
    EasyMock.replay(tabletMeta);

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> results =
        UpdateTablets.getNewTabletFiles(newExtents, tabletMeta, firstAndLastKeys::get);

    assertEquals(expected.keySet(), results.keySet());
    expected.forEach(((extent, files) -> {
      assertEquals(files, results.get(extent));
    }));

    // Test a tablet with no files going to it

    var tabletFiles2 = Map.of(file2, tabletFiles.get(file2), file4, tabletFiles.get(file4));
    ke1Expected = Map.of(file2, new DataFileValue(1000, 100, 50));
    ke2Expected = Map.of(file2, new DataFileValue(1000, 100, 50));
    ke3Expected = Map.of(file4, new DataFileValue(4000, 400));
    ke4Expected = Map.of();
    expected = Map.of(ke1, ke1Expected, ke2, ke2Expected, ke3, ke3Expected, ke4, ke4Expected);

    tabletMeta = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tabletMeta.getFilesMap()).andReturn(tabletFiles2).anyTimes();
    EasyMock.replay(tabletMeta);

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> results2 =
        UpdateTablets.getNewTabletFiles(newExtents, tabletMeta, firstAndLastKeys::get);
    assertEquals(expected.keySet(), results2.keySet());
    expected.forEach(((extent, files) -> {
      assertEquals(files, results2.get(extent));
    }));

  }
}
