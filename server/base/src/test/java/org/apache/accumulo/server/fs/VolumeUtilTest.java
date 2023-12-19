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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class VolumeUtilTest {

  @Test
  public void testSwitchVolume() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/b/accumulo")));

    assertEquals(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/b/accumulo/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));

    replacements.clear();
    replacements
        .add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/b/accumulo")));

    assertEquals(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"),
        VolumeUtil.switchVolume(new Path("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf"),
            FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/b/accumulo/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
  }

  @Test
  public void testSwitchVolumesDifferentSourceDepths() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/b")));

    assertEquals(new Path("viewfs:/a/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/a/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/b/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/a/tables/t-00000/C000.rf"), FileType.TABLE,
        replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));

    replacements.clear();
    replacements.add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/b")));

    assertEquals(new Path("viewfs:/a/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/a/tables/t-00000/C000.rf"),
        VolumeUtil.switchVolume(new Path("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf"),
            FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/b/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/a/tables/t-00000/C000.rf"), FileType.TABLE,
        replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
  }

  @Test
  public void testSwitchVolumesDifferentTargetDepths() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/path3")));

    assertEquals(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/path3/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));

    replacements.clear();
    replacements
        .add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/path3")));

    assertEquals(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"),
        VolumeUtil.switchVolume(new Path("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf"),
            FileType.TABLE, replacements));
    assertEquals(new Path("viewfs:/path3/tables/t-00000/C000.rf"), VolumeUtil.switchVolume(
        new Path("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf"), FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("viewfs:/path1/path2/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("file:/nn1/a/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
    assertNull(VolumeUtil.switchVolume(new Path("hdfs://nn1/accumulo/tables/t-00000/C000.rf"),
        FileType.TABLE, replacements));
  }

  @Test
  public void testRootTableReplacement() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("file:/foo/v1"), new Path("file:/foo/v8")));
    replacements.add(new Pair<>(new Path("file:/foo/v2"), new Path("file:/foo/v9")));

    FileType ft = FileType.TABLE;

    assertEquals(new Path("file:/foo/v8/tables/+r/root_tablet"),
        VolumeUtil.switchVolume(new Path("file:/foo/v1/tables/+r/root_tablet"), ft, replacements));
  }

  @Test
  public void testWalVolumeReplacment() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements
        .add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/b/accumulo")));

    String walUUID = UUID.randomUUID().toString();
    String fileName = "hdfs://nn1/accumulo/wal/localhost+9997/" + walUUID;
    LogEntry le = LogEntry.fromPath(fileName);
    LogEntry fixedVolume = VolumeUtil.switchVolumes(le, replacements);
    assertEquals("viewfs:/a/accumulo/wal/localhost+9997/" + walUUID, fixedVolume.getPath());

    fileName = "hdfs://nn1:9000/accumulo/wal/localhost+9997/" + walUUID;
    le = LogEntry.fromPath(fileName);
    fixedVolume = VolumeUtil.switchVolumes(le, replacements);
    assertEquals("viewfs:/a/accumulo/wal/localhost+9997/" + walUUID, fixedVolume.getPath());

    fileName = "hdfs://nn2/accumulo/wal/localhost+9997/" + walUUID;
    le = LogEntry.fromPath(fileName);
    fixedVolume = VolumeUtil.switchVolumes(le, replacements);
    assertEquals("viewfs:/b/accumulo/wal/localhost+9997/" + walUUID, fixedVolume.getPath());
  }
}
