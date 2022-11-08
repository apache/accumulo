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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.hadoop.conf.Configuration;
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
}
