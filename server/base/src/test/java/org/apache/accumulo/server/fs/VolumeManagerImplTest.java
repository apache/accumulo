/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class VolumeManagerImplTest {

  protected VolumeManager fs;
  private Configuration hadoopConf = new Configuration();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    fs = VolumeManagerImpl.getLocal(System.getProperty("user.dir"));
  }

  @Test
  public void invalidChooserConfigured() throws Exception {
    List<String> volumes = Arrays.asList("file://one/", "file://two/", "file://three/");
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(INSTANCE_DFS_URI, volumes.get(0));
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", volumes));
    conf.set(Property.GENERAL_VOLUME_CHOOSER,
        "org.apache.accumulo.server.fs.ChooserThatDoesntExist");
    thrown.expect(RuntimeException.class);
    VolumeManagerImpl.get(conf, hadoopConf);
  }

  @Test
  public void noViewFS() throws Exception {
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(Property.INSTANCE_VOLUMES, "viewfs://dummy");
    thrown.expect(IllegalArgumentException.class);
    VolumeManagerImpl.get(conf, hadoopConf);
  }

  public static class WrongVolumeChooser implements VolumeChooser {
    @Override
    public String choose(VolumeChooserEnvironment env, String[] options) {
      return "file://totally-not-given/";
    }

    @Override
    public String[] choosable(VolumeChooserEnvironment env, String[] options) {
      return new String[] {"file://totally-not-given"};
    }
  }

  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;

  // Expected to throw a runtime exception when the WrongVolumeChooser picks an invalid volume.
  @Test
  public void chooseFromOptions() throws Exception {
    List<String> volumes = Arrays.asList("file://one/", "file://two/", "file://three/");
    ConfigurationCopy conf = new ConfigurationCopy();
    conf.set(INSTANCE_DFS_URI, volumes.get(0));
    conf.set(Property.INSTANCE_VOLUMES, String.join(",", volumes));
    conf.set(Property.GENERAL_VOLUME_CHOOSER, WrongVolumeChooser.class.getName());
    thrown.expect(RuntimeException.class);
    VolumeManager vm = VolumeManagerImpl.get(conf, hadoopConf);
    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(TableId.of("sometable"), null, null);
    String choice = vm.choose(chooserEnv, volumes.toArray(new String[0]));
    assertTrue("shouldn't see invalid options from misbehaving chooser.", volumes.contains(choice));
  }
}
