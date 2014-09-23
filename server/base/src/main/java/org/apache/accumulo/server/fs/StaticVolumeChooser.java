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

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration.AllFilter;
import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.log4j.Logger;

public class StaticVolumeChooser implements VolumeChooser {
  private static final Logger log = Logger.getLogger(StaticVolumeChooser.class);

  public StaticVolumeChooser() {}

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    // Get the current table's properties, and find the preferred volumes property
    PropertyFilter filter = new AllFilter();
    Map<String,String> props = new java.util.HashMap<String,String>();
    env.getProperties(props, filter);
    String volume = props.get("table.custom.preferredVolumes");
    log.info("In custom chooser");
    log.info("Volumes: " + volume);
    log.info("TableID: " + env.getTableId());

    if (volume != null) {
      // If preferred volume is specified, make sure that volume is an instance volume.
      boolean usePreferred = false;
      for (String s : options) {
        if (volume.equals(s)) {
          usePreferred = true;
          break;
        }
      }

      // If the preferred volume is an instance volume return that volume.
      if (usePreferred)
        return volume;
      // Otherwise warn the user that it is not an instance volume and that the chooser will default to randomly choosing from the instance volumes.
      else
        log.warn("Preferred volume, " + volume + ", is not an instance volume.  Defaulting to randomly choosing from instance volumes");
    }
    // If the preferred volumes property is not specified, warn the user, then use a RandomVolumeChoser to choose randomly from the given list of volumes
    log.warn("No preferred volume specified.  Defaulting to randomly choosing from instance volumes");
    return new RandomVolumeChooser().choose(options);
  }

  @Override
  public String choose(String[] options) {
    // If table is not specified, use a RandomVolumeChoser to choose randomly from the given options
    return new RandomVolumeChooser().choose(options);
  }
}
