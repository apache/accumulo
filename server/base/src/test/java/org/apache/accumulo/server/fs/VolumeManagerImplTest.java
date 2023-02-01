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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.volume.Volume;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class VolumeManagerImplTest {

  private Configuration hadoopConf = new Configuration();

  @Test
  public void invalidChooserConfigured() throws Exception {
    List<String> volumes = Arrays.asList("file://one/", "file://two/", "file://three/");
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", volumes));
    conf.set(Property.GENERAL_VOLUME_CHOOSER,
        "org.apache.accumulo.server.fs.ChooserThatDoesntExist");
    assertThrows(RuntimeException.class, () -> VolumeManagerImpl.get(conf, hadoopConf));
  }

  @Test
  public void noViewFS() throws Exception {
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, "viewfs://dummy");
    assertThrows(IllegalArgumentException.class, () -> VolumeManagerImpl.get(conf, hadoopConf));
  }

  public static class WrongVolumeChooser implements VolumeChooser {
    @Override
    public String choose(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
        Set<String> options) {
      return "file://totally-not-given/";
    }

    @Override
    public Set<String> choosable(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
        Set<String> options) {
      return Set.of("file://totally-not-given");
    }
  }

  // Expected to throw a runtime exception when the WrongVolumeChooser picks an invalid volume.
  @Test
  public void chooseFromOptions() throws Exception {
    Set<String> volumes = Set.of("file://one/", "file://two/", "file://three/");
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", volumes));
    conf.set(Property.GENERAL_VOLUME_CHOOSER, WrongVolumeChooser.class.getName());
    try (var vm = VolumeManagerImpl.get(conf, hadoopConf)) {
      org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment chooserEnv =
          new VolumeChooserEnvironment() {

            @Override
            public Optional<TableId> getTable() {
              throw new UnsupportedOperationException();
            }

            @Override
            public ServiceEnvironment getServiceEnv() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Text getEndRow() {
              throw new UnsupportedOperationException();
            }

            @Override
            public Scope getChooserScope() {
              throw new UnsupportedOperationException();
            }
          };
      assertThrows(RuntimeException.class, () -> vm.choose(chooserEnv, volumes));
    }
  }

  @Test
  public void testConfigurationOverrides() throws Exception {

    final String vol1 = "file://127.0.0.1/vol1/";
    final String vol2 = "file://localhost/vol2/";
    final String vol3 = "hdfs://127.0.0.1/accumulo";
    final String vol4 = "hdfs://localhost/accumulo";

    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", vol1, vol2, vol3, vol4));
    conf.set(Property.GENERAL_VOLUME_CHOOSER, Property.GENERAL_VOLUME_CHOOSER.getDefaultValue());
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol1 + "."
        + HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, "10");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol1 + "."
        + HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS, "true");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol2 + "."
        + HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, "20");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol2 + "."
        + HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS, "false");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol3 + "."
        + HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, "30");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol3 + "."
        + HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS, "TRUE");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol4 + "."
        + HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, "40");
    conf.set(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + vol4 + "."
        + HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS, "FALSE");

    VolumeManager vm = VolumeManagerImpl.get(conf, hadoopConf);

    FileSystem fs1 = vm.getFileSystemByPath(new Path(vol1));
    Configuration conf1 = fs1.getConf();

    FileSystem fs2 = vm.getFileSystemByPath(new Path(vol2));
    Configuration conf2 = fs2.getConf();

    FileSystem fs3 = vm.getFileSystemByPath(new Path(vol3));
    Configuration conf3 = fs3.getConf();

    FileSystem fs4 = vm.getFileSystemByPath(new Path(vol4));
    Configuration conf4 = fs4.getConf();

    assertEquals("10", conf1.get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
    assertEquals("true", conf1.get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));

    assertEquals("20", conf2.get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
    assertEquals("false", conf2.get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));

    assertEquals("30", conf3.get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
    assertEquals("TRUE", conf3.get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));

    assertEquals("40", conf4.get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
    assertEquals("FALSE", conf4.get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));

    Collection<Volume> vols = vm.getVolumes();
    assertEquals(4, vols.size());
    vols.forEach(v -> {
      if (v.containsPath(new Path(vol1))) {
        assertEquals("10",
            v.getFileSystem().getConf().get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
        assertEquals("true", v.getFileSystem().getConf()
            .get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));
      } else if (v.containsPath(new Path(vol2))) {
        assertEquals("20",
            v.getFileSystem().getConf().get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
        assertEquals("false", v.getFileSystem().getConf()
            .get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));
      } else if (v.containsPath(new Path(vol3))) {
        assertEquals("30",
            v.getFileSystem().getConf().get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
        assertEquals("TRUE", v.getFileSystem().getConf()
            .get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));
      } else if (v.containsPath(new Path(vol4))) {
        assertEquals("40",
            v.getFileSystem().getConf().get(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY));
        assertEquals("FALSE", v.getFileSystem().getConf()
            .get(HdfsClientConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS));
      } else {
        fail("Unhandled volume: " + v);
      }
    });

  }
}
