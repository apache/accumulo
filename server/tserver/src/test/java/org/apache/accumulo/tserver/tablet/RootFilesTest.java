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
package org.apache.accumulo.tserver.tablet;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.RandomVolumeChooser;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class RootFilesTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private class TestWrapper {
    File rootTabletDir;
    Set<FileRef> oldDatafiles;
    String compactName;
    FileRef tmpDatafile;
    FileRef newDatafile;
    VolumeManager vm;
    AccumuloConfiguration conf;

    TestWrapper(VolumeManager vm, AccumuloConfiguration conf, String compactName, String... inputFiles) throws IOException {
      this.vm = vm;
      this.conf = conf;

      rootTabletDir = new File(tempFolder.newFolder(), "accumulo/tables/+r/root_tablet");
      assertTrue(rootTabletDir.mkdirs() || rootTabletDir.isDirectory());
      oldDatafiles = new HashSet<>();
      for (String filename : inputFiles) {
        File file = new File(rootTabletDir, filename);
        assertTrue(file.createNewFile());
        oldDatafiles.add(new FileRef(file.toURI().toString()));
      }

      this.compactName = compactName;

      File tmpFile = new File(rootTabletDir, compactName + "_tmp");
      assertTrue(tmpFile.createNewFile());
      tmpDatafile = new FileRef(tmpFile.toURI().toString());

      newDatafile = new FileRef(new File(rootTabletDir, compactName).toURI().toString());
    }

    void prepareReplacement() throws IOException {
      RootFiles.prepareReplacement(vm, new Path(rootTabletDir.toURI()), oldDatafiles, compactName);
    }

    void renameReplacement() throws IOException {
      RootFiles.renameReplacement(vm, tmpDatafile, newDatafile);
    }

    public void finishReplacement() throws IOException {
      RootFiles.finishReplacement(conf, vm, new Path(rootTabletDir.toURI()), oldDatafiles, compactName);
    }

    public Collection<String> cleanupReplacement(String... expectedFiles) throws IOException {
      Collection<String> ret = RootFiles.cleanupReplacement(vm, vm.listStatus(new Path(rootTabletDir.toURI())), true);

      HashSet<String> expected = new HashSet<>();
      for (String efile : expectedFiles)
        expected.add(new File(rootTabletDir, efile).toURI().toString());

      Assert.assertEquals(expected, new HashSet<>(ret));

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

      Assert.assertEquals(expected, actual);
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testFileReplacement() throws IOException {

    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_DFS_URI, "file:///");
    conf.set(Property.INSTANCE_DFS_DIR, "/");
    conf.set(Property.GENERAL_VOLUME_CHOOSER, RandomVolumeChooser.class.getName());

    VolumeManager vm = VolumeManagerImpl.get(conf);

    TestWrapper wrapper = new TestWrapper(vm, conf, "A00004.rf", "A00002.rf", "F00003.rf");
    wrapper.prepareReplacement();
    wrapper.renameReplacement();
    wrapper.finishReplacement();
    wrapper.assertFiles("A00004.rf");

    wrapper = new TestWrapper(vm, conf, "A00004.rf", "A00002.rf", "F00003.rf");
    wrapper.prepareReplacement();
    wrapper.cleanupReplacement("A00002.rf", "F00003.rf");
    wrapper.assertFiles("A00002.rf", "F00003.rf");

    wrapper = new TestWrapper(vm, conf, "A00004.rf", "A00002.rf", "F00003.rf");
    wrapper.prepareReplacement();
    wrapper.renameReplacement();
    wrapper.cleanupReplacement("A00004.rf");
    wrapper.assertFiles("A00004.rf");

    wrapper = new TestWrapper(vm, conf, "A00004.rf", "A00002.rf", "F00003.rf");
    wrapper.prepareReplacement();
    wrapper.renameReplacement();
    wrapper.finishReplacement();
    wrapper.cleanupReplacement("A00004.rf");
    wrapper.assertFiles("A00004.rf");

  }
}
