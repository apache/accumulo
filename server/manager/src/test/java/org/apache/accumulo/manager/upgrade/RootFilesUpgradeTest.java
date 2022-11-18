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
package org.apache.accumulo.manager.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.fs.RandomVolumeChooser;
import org.apache.accumulo.manager.WithTestNames;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class RootFilesUpgradeTest extends WithTestNames {

  @TempDir
  private static File tempDir;

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  private class TestWrapper {
    File rootTabletDir;
    Set<Path> oldDatafiles;
    String compactName;
    Path tmpDatafile;
    Path newDatafile;
    VolumeManager vm;
    AccumuloConfiguration conf;

    public void prepareReplacement(VolumeManager fs, Path location, Set<Path> oldDatafiles,
        String compactName) throws IOException {
      for (Path path : oldDatafiles) {
        rename(fs, path, new Path(location + "/delete+" + compactName + "+" + path.getName()));
      }
    }

    public void renameReplacement(VolumeManager fs, Path tmpDatafile, Path newDatafile)
        throws IOException {
      if (fs.exists(newDatafile)) {
        throw new IllegalStateException("Target map file already exist " + newDatafile);
      }

      rename(fs, tmpDatafile, newDatafile);
    }

    public void finishReplacement(AccumuloConfiguration acuTableConf, VolumeManager fs,
        Path location, Set<Path> oldDatafiles, String compactName) throws IOException {
      // start deleting files, if we do not finish they will be cleaned
      // up later
      for (Path path : oldDatafiles) {
        Path deleteFile = new Path(location + "/delete+" + compactName + "+" + path.getName());
        if (acuTableConf.getBoolean(Property.GC_TRASH_IGNORE) || !fs.moveToTrash(deleteFile)) {
          fs.deleteRecursively(deleteFile);
        }
      }
    }

    TestWrapper(VolumeManager vm, AccumuloConfiguration conf, String dirName, String compactName,
        String... inputFiles) throws IOException {
      this.vm = vm;
      this.conf = conf;

      rootTabletDir = new File(tempDir, dirName + "/accumulo/tables/+r/root_tablet");
      assertTrue(rootTabletDir.mkdirs() || rootTabletDir.isDirectory());
      oldDatafiles = new HashSet<>();
      for (String filename : inputFiles) {
        File file = new File(rootTabletDir, filename);
        assertTrue(file.createNewFile());
        oldDatafiles.add(new Path(file.toURI()));
      }

      this.compactName = compactName;

      File tmpFile = new File(rootTabletDir, compactName + "_tmp");
      assertTrue(tmpFile.createNewFile());
      tmpDatafile = new Path(tmpFile.toURI());

      newDatafile = new Path(new File(rootTabletDir, compactName).toURI());
    }

    void prepareReplacement() throws IOException {
      prepareReplacement(vm, new Path(rootTabletDir.toURI()), oldDatafiles, compactName);
    }

    void renameReplacement() throws IOException {
      renameReplacement(vm, tmpDatafile, newDatafile);
    }

    public void finishReplacement() throws IOException {
      finishReplacement(conf, vm, new Path(rootTabletDir.toURI()), oldDatafiles, compactName);
    }

    public Collection<String> cleanupReplacement(String... expectedFiles) throws IOException {
      Collection<String> ret =
          Upgrader9to10.cleanupRootTabletFiles(vm, rootTabletDir.toString()).keySet();

      HashSet<String> expected = new HashSet<>();
      for (String efile : expectedFiles) {
        expected.add(new File(rootTabletDir, efile).toURI().toString());
      }

      assertEquals(expected, new HashSet<>(ret));

      return ret;
    }

    public void assertFiles(String... files) {
      HashSet<String> actual = new HashSet<>();
      File[] children = rootTabletDir.listFiles();
      if (children != null) {
        for (File file : children) {
          actual.add(file.getName());
        }
      }

      HashSet<String> expected = new HashSet<>();
      expected.addAll(Arrays.asList(files));

      assertEquals(expected, actual);
    }
  }

  @Test
  public void testFileReplacement() throws IOException {

    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, "file:///");
    conf.set(Property.GENERAL_VOLUME_CHOOSER, RandomVolumeChooser.class.getName());

    try (var vm = VolumeManagerImpl.getLocalForTesting("file:///")) {

      String[] uniqueDirNames = getUniqueNames(4);

      TestWrapper wrapper =
          new TestWrapper(vm, conf, uniqueDirNames[0], "A00004.rf", "A00002.rf", "F00003.rf");
      wrapper.prepareReplacement();
      wrapper.renameReplacement();
      wrapper.finishReplacement();
      wrapper.assertFiles("A00004.rf");

      wrapper = new TestWrapper(vm, conf, uniqueDirNames[1], "A00004.rf", "A00002.rf", "F00003.rf");
      wrapper.prepareReplacement();
      wrapper.cleanupReplacement("A00002.rf", "F00003.rf");
      wrapper.assertFiles("A00002.rf", "F00003.rf");

      wrapper = new TestWrapper(vm, conf, uniqueDirNames[2], "A00004.rf", "A00002.rf", "F00003.rf");
      wrapper.prepareReplacement();
      wrapper.renameReplacement();
      wrapper.cleanupReplacement("A00004.rf");
      wrapper.assertFiles("A00004.rf");

      wrapper = new TestWrapper(vm, conf, uniqueDirNames[3], "A00004.rf", "A00002.rf", "F00003.rf");
      wrapper.prepareReplacement();
      wrapper.renameReplacement();
      wrapper.finishReplacement();
      wrapper.cleanupReplacement("A00004.rf");
      wrapper.assertFiles("A00004.rf");
    }
  }

  public String[] getUniqueNames(int numOfNames) {
    String[] result = new String[numOfNames];
    for (int i = 0; i < result.length; i++) {
      result[i] = testName() + i;
    }
    return result;
  }
}
