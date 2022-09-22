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
package org.apache.accumulo.server.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class FileTypeTest {
  @Test
  public void testVolumeExtraction() {
    assertEquals(new Path("file:/a/accumulo"),
        FileType.TABLE.getVolume(new Path("file:/a/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:///a/accumulo"),
        FileType.TABLE.getVolume(new Path("file:/a/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:///a/accumulo"),
        FileType.TABLE.getVolume(new Path("file:///a/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:/a/accumulo"),
        FileType.TABLE.getVolume(new Path("file:///a/accumulo/tables/2b/t-001/C00.rf")));

    // Having an 'accumulo' directory is not a requirement
    assertEquals(new Path("file:/a"),
        FileType.TABLE.getVolume(new Path("file:/a/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:///a"),
        FileType.TABLE.getVolume(new Path("file:/a/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:///a"),
        FileType.TABLE.getVolume(new Path("file:///a/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:/a"),
        FileType.TABLE.getVolume(new Path("file:///a/tables/2b/t-001/C00.rf")));

    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("file:/a/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("file:///a/accumulo/tables/2b/t-001/C00.rf")));

    // Having an 'accumulo' directory is not a requirement
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("file:/a/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("file:///a/tables/2b/t-001/C00.rf")));

    assertEquals(new Path("file:/accumulo"),
        FileType.TABLE.getVolume(new Path("file:/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:/accumulo"),
        FileType.TABLE.getVolume(new Path("file:///accumulo/tables/2b/t-001/C00.rf")));

    // Having an 'accumulo' directory is not a requirement
    assertEquals(new Path("file:/"),
        FileType.TABLE.getVolume(new Path("file:/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("file:/"),
        FileType.TABLE.getVolume(new Path("file:///tables/2b/t-001/C00.rf")));

    assertEquals(new Path("file:/a"),
        FileType.WAL.getVolume(new Path("file:/a/wal/1.2.3.4/aaa-bbb-ccc-ddd")));

    assertNull(FileType.WAL.getVolume(new Path("1.2.3.4/aaa-bbb-ccc-ddd")));
    assertNull(FileType.TABLE.getVolume(new Path("../2b/t-001/C00.rf")));
    assertNull(FileType.TABLE.removeVolume(new Path("../2b/t-001/C00.rf")));
    assertNull(FileType.TABLE.getVolume(new Path("/t-001/C00.rf")));
    assertNull(FileType.TABLE.removeVolume(new Path("/t-001/C00.rf")));

    assertEquals(new Path("hdfs://nn1/accumulo"),
        FileType.TABLE.getVolume(new Path("hdfs://nn1/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("hdfs://nn1/a/accumulo"),
        FileType.TABLE.getVolume(new Path("hdfs://nn1/a/accumulo/tables/2b/t-001/C00.rf")));

    // Having an 'accumulo' directory is not a requirement
    assertEquals(new Path("hdfs://nn1/"),
        FileType.TABLE.getVolume(new Path("hdfs://nn1/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("hdfs://nn1/a"),
        FileType.TABLE.getVolume(new Path("hdfs://nn1/a/tables/2b/t-001/C00.rf")));

    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("hdfs://nn1/accumulo/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("hdfs://nn1/a/accumulo/tables/2b/t-001/C00.rf")));

    // Having an 'accumulo' directory is not a requirement
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("hdfs://nn1/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("tables/2b/t-001/C00.rf"),
        FileType.TABLE.removeVolume(new Path("hdfs://nn1/a/tables/2b/t-001/C00.rf")));

    assertEquals(new Path("tables"),
        FileType.TABLE.removeVolume(new Path("file:/a/accumulo/tables")));
    assertEquals(new Path("tables/"),
        FileType.TABLE.removeVolume(new Path("file:/a/accumulo/tables/")));
    assertEquals(new Path("file:/a/accumulo"),
        FileType.TABLE.getVolume(new Path("file:/a/accumulo/tables/")));
    assertEquals(null, FileType.TABLE.getVolume(new Path("/a/accumulo/tables2/")));
    assertEquals(new Path("tables/2b/t-001/C00.rf"), FileType.TABLE
        .removeVolume(new Path("file:/a/accumulo/tablesstuff/tables/2b/t-001/C00.rf")));
    assertEquals(new Path("tables/tablestuff2"),
        FileType.TABLE.removeVolume(new Path("file:/a/accumulo/tablesstuff/tables/tablestuff2")));
    assertEquals(new Path("tables/tables"),
        FileType.TABLE.removeVolume(new Path("file:/a/accumulo/tablesstuff/tables/tables")));
  }
}
