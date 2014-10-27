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

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;

public class PerTableVolumeChooser implements VolumeChooser {

  private static final VolumeChooser fallbackVolumeChooser = new RandomVolumeChooser();

  public PerTableVolumeChooser() {}

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    VolumeChooser chooser;
    if (env.hasTableId()) {
      TableConfiguration conf = new ServerConfigurationFactory(HdfsZooInstance.getInstance()).getTableConfiguration(env.getTableId());
      chooser = Property.createTableInstanceFromPropertyName(conf, Property.TABLE_VOLUME_CHOOSER, VolumeChooser.class, fallbackVolumeChooser);
    } else {
      chooser = fallbackVolumeChooser;
    }

    return chooser.choose(env, options);
  }
}
