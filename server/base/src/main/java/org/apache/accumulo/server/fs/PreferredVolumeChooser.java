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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.log4j.Logger;

public class PreferredVolumeChooser extends RandomVolumeChooser implements VolumeChooser {
  private static final Logger log = Logger.getLogger(PreferredVolumeChooser.class);

  public static final String PREFERRED_VOLUMES_CUSTOM_KEY = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "preferredVolumes";

  public PreferredVolumeChooser() {}

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    if (!env.hasTableId())
      return super.choose(env, options);

    // Get the current table's properties, and find the preferred volumes property
    TableConfiguration config = new ServerConfigurationFactory(HdfsZooInstance.getInstance()).getTableConfiguration(env.getTableId());
    PropertyFilter filter = new PropertyFilter() {
      @Override
      public boolean accept(String key) {
        return PREFERRED_VOLUMES_CUSTOM_KEY.equals(key);
      }
    };
    Map<String,String> props = new HashMap<>();
    config.getProperties(props, filter);
    if (props.isEmpty()) {
      log.warn("No preferred volumes specified. Defaulting to randomly choosing from instance volumes");
      return super.choose(env, options);
    }
    String volumes = props.get(PREFERRED_VOLUMES_CUSTOM_KEY);
    log.trace("In custom chooser");
    log.trace("Volumes: " + volumes);
    log.trace("TableID: " + env.getTableId());

    ArrayList<String> prefVol = new ArrayList<String>();
    // If the preferred volumes property is specified, split the returned string by the comma and add them to a preferred volumes list
    prefVol.addAll(Arrays.asList(volumes.split(",")));

    // Change the given array to a List and only keep the preferred volumes that are in the given array.
    prefVol.retainAll(Arrays.asList(options));

    // If there are no preferred volumes left, then warn the user and choose randomly from the instance volumes
    if (prefVol.isEmpty()) {
      log.warn("Preferred volumes are not instance volumes. Defaulting to randomly choosing from instance volumes");
      return super.choose(env, options);
    }

    // Randomly choose the volume from the preferred volumes
    String choice = prefVol.get(random.nextInt(prefVol.size()));
    log.trace("Choice = " + choice);
    return choice;
  }
}
