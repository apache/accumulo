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
package org.apache.accumulo.server.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class VolumeUtilTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testSwitchVolume() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/b/accumulo")));

    Assert.assertEquals("viewfs:/a/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/a/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/b/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));

    replacements.clear();
    replacements.add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/a/accumulo")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/b/accumulo")));

    Assert.assertEquals("viewfs:/a/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/a/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/b/accumulo/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
  }

  @Test
  public void testSwitchVolumesDifferentSourceDepths() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/b")));

    Assert
        .assertEquals("viewfs:/a/tables/t-00000/C000.rf", VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/a/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert
        .assertEquals("viewfs:/b/tables/t-00000/C000.rf", VolumeUtil.switchVolume("hdfs://nn2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/a/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));

    replacements.clear();
    replacements.add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/a")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/b")));

    Assert.assertEquals("viewfs:/a/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/a/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/b/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/a/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
  }

  @Test
  public void testSwitchVolumesDifferentTargetDepths() {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("hdfs://nn1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/accumulo"), new Path("viewfs:/path3")));

    Assert.assertEquals("viewfs:/path1/path2/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/path1/path2/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/path3/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/path1/path2/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));

    replacements.clear();
    replacements.add(new Pair<>(new Path("hdfs://nn1/d1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn1:9000/d1/accumulo"), new Path("viewfs:/path1/path2")));
    replacements.add(new Pair<>(new Path("hdfs://nn2/d2/accumulo"), new Path("viewfs:/path3")));

    Assert.assertEquals("viewfs:/path1/path2/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/path1/path2/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn1:9000/d1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertEquals("viewfs:/path3/tables/t-00000/C000.rf",
        VolumeUtil.switchVolume("hdfs://nn2/d2/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("viewfs:/path1/path2/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("file:/nn1/a/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
    Assert.assertNull(VolumeUtil.switchVolume("hdfs://nn1/accumulo/tables/t-00000/C000.rf", FileType.TABLE, replacements));
  }

  @Test
  public void testSame() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());

    Path subdir1 = new Path(tempFolder.newFolder().toURI());
    Path subdir2 = new Path(tempFolder.newFolder().toURI());
    Path subdir3 = new Path(tempFolder.newFolder().toURI());

    Assert.assertFalse(VolumeUtil.same(fs, subdir1, fs, new Path(tempFolder.getRoot().toURI().toString(), "8854339269459287524098238497")));
    Assert.assertFalse(VolumeUtil.same(fs, new Path(tempFolder.getRoot().toURI().toString(), "8854339269459287524098238497"), fs, subdir1));
    Assert.assertTrue(VolumeUtil.same(fs, subdir1, fs, subdir1));

    writeFile(fs, subdir1, "abc", "foo");
    writeFile(fs, subdir2, "abc", "bar");
    writeFile(fs, subdir3, "abc", "foo");

    Assert.assertTrue(VolumeUtil.same(fs, subdir1, fs, subdir1));
    Assert.assertFalse(VolumeUtil.same(fs, subdir1, fs, subdir2));
    Assert.assertFalse(VolumeUtil.same(fs, subdir2, fs, subdir1));
    Assert.assertTrue(VolumeUtil.same(fs, subdir1, fs, subdir3));
    Assert.assertTrue(VolumeUtil.same(fs, subdir3, fs, subdir1));

    writeFile(fs, subdir1, "def", "123456");
    writeFile(fs, subdir2, "def", "123456");
    writeFile(fs, subdir3, "def", "123456");

    Assert.assertTrue(VolumeUtil.same(fs, subdir1, fs, subdir1));
    Assert.assertFalse(VolumeUtil.same(fs, subdir1, fs, subdir2));
    Assert.assertFalse(VolumeUtil.same(fs, subdir2, fs, subdir1));
    Assert.assertTrue(VolumeUtil.same(fs, subdir1, fs, subdir3));
    Assert.assertTrue(VolumeUtil.same(fs, subdir3, fs, subdir1));

    writeFile(fs, subdir3, "ghi", "09876");

    Assert.assertFalse(VolumeUtil.same(fs, subdir1, fs, subdir3));
    Assert.assertFalse(VolumeUtil.same(fs, subdir3, fs, subdir1));

    fs.mkdirs(new Path(subdir2, "dir1"));

    try {
      VolumeUtil.same(fs, subdir1, fs, subdir2);
      Assert.fail();
    } catch (IllegalArgumentException e) {}

    try {
      VolumeUtil.same(fs, subdir2, fs, subdir1);
      Assert.fail();
    } catch (IllegalArgumentException e) {}

    try {
      VolumeUtil.same(fs, subdir1, fs, new Path(subdir2, "def"));
      Assert.fail();
    } catch (IllegalArgumentException e) {}

    try {
      VolumeUtil.same(fs, new Path(subdir2, "def"), fs, subdir3);
      Assert.fail();
    } catch (IllegalArgumentException e) {}

  }

  @Test
  public void testRootTableReplacement() throws IOException {
    List<Pair<Path,Path>> replacements = new ArrayList<>();
    replacements.add(new Pair<>(new Path("file:/foo/v1"), new Path("file:/foo/v8")));
    replacements.add(new Pair<>(new Path("file:/foo/v2"), new Path("file:/foo/v9")));

    FileType ft = FileType.TABLE;

    Assert.assertEquals("file:/foo/v8/tables/+r/root_tablet", VolumeUtil.switchVolume("file:/foo/v1/tables/+r/root_tablet", ft, replacements));
  }

  private void writeFile(FileSystem fs, Path dir, String filename, String data) throws IOException {
    FSDataOutputStream out = fs.create(new Path(dir, filename));
    try {
      out.writeUTF(data);
    } finally {
      out.close();
    }
  }
}
