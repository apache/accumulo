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

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class VolumeManagerImplTest {

  protected VolumeManager fs;

  @Before
  public void setup() throws Exception {
    fs = VolumeManagerImpl.getLocal(System.getProperty("user.dir"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void defaultTabletDirWithoutTableId() throws Exception {
    fs.getFullPath(FileType.TABLE, "/default_tablet/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void tabletDirWithoutTableId() throws Exception {
    fs.getFullPath(FileType.TABLE, "/t-0000001/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void defaultTabletFileWithoutTableId() throws Exception {
    fs.getFullPath(FileType.TABLE, "/default_tablet/C0000001.rf");
  }

  @Test(expected = IllegalArgumentException.class)
  public void tabletFileWithoutTableId() throws Exception {
    fs.getFullPath(FileType.TABLE, "/t-0000001/C0000001.rf");
  }

  @Test
  public void tabletDirWithTableId() throws Exception {
    String basePath = fs.getDefaultVolume().getBasePath();
    String scheme = fs.getDefaultVolume().getFileSystem().getUri().toURL().getProtocol();
    System.out.println(basePath);
    Path expectedBase = new Path(scheme + ":" + basePath, FileType.TABLE.getDirectory());
    List<String> pathsToTest = Arrays.asList("1/default_tablet", "1/default_tablet/", "1/t-0000001");
    for (String pathToTest : pathsToTest) {
      Path fullPath = fs.getFullPath(FileType.TABLE, pathToTest);
      Assert.assertEquals(new Path(expectedBase, pathToTest), fullPath);
    }
  }

  @Test
  public void tabletFileWithTableId() throws Exception {
    String basePath = fs.getDefaultVolume().getBasePath();
    String scheme = fs.getDefaultVolume().getFileSystem().getUri().toURL().getProtocol();
    System.out.println(basePath);
    Path expectedBase = new Path(scheme + ":" + basePath, FileType.TABLE.getDirectory());
    List<String> pathsToTest = Arrays.asList("1/default_tablet/C0000001.rf", "1/t-0000001/C0000001.rf");
    for (String pathToTest : pathsToTest) {
      Path fullPath = fs.getFullPath(FileType.TABLE, pathToTest);
      Assert.assertEquals(new Path(expectedBase, pathToTest), fullPath);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void noViewFS() throws Exception {
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, "viewfs://dummy");
    VolumeManagerImpl.get(conf);
  }

  public static class WrongVolumeChooser implements VolumeChooser {
    @Override
    public String choose(String[] options) {
      return "file://totally-not-given/";
    }
  }

  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;

  @Test
  public void chooseFromOptions() throws Exception {
    List<String> volumes = Arrays.asList("file://one/", "file://two/", "file://three/");
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(INSTANCE_DFS_URI, volumes.get(0));
    conf.set(Property.INSTANCE_VOLUMES, StringUtils.join(volumes, ","));
    conf.set(Property.GENERAL_VOLUME_CHOOSER, WrongVolumeChooser.class.getName());
    VolumeManager vm = VolumeManagerImpl.get(conf);
    String choice = vm.choose(volumes.toArray(new String[0]));
    Assert.assertTrue("shouldn't see invalid options from misbehaving chooser.", volumes.contains(choice));
  }
}
