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
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.conf.AccumuloConfiguration.AllFilter;
import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.log4j.Logger;

public class RandomVolumeChooser implements VolumeChooser {
  private static final Logger log = Logger.getLogger(RandomVolumeChooser.class);
  Random random = new Random();

  public RandomVolumeChooser() {}

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    // Get the current table's properties, and find the preferred volumes property
    PropertyFilter filter = new AllFilter();
    Map<String,String> props = new java.util.HashMap<String,String>();
    ArrayList<String> prefVol = new ArrayList<String>();
    env.getProperties(props, filter);
    String volumes = props.get("table.custom.preferredVolumes");
    log.info("In Random Chooser");
    log.info("TableID: " + env.getTableId());
    log.info("Volumes: " + volumes);

    if (volumes != null) {
      // If the preferred volumes property is specified, split the returned string by the comma and add them to a preferred volumes list
      for (String s : volumes.split(","))
        prefVol.add(s);

      // Change the given array to a List and only keep the preferred volumes that are in the given array.
      List<String> op = Arrays.asList(options);
      prefVol.retainAll(op);

      // If there are no preferred volumes left, then warn the user and choose randomly from the instance volumes
      if (prefVol.size() == 0) {
        log.warn("Preferred volumes are not instance volumes.  Defaulting to randomly choosing from instance volumes");
        return choose(options);
      }

      // Randomly choose the volume from the preferred volumes
      int choice = random.nextInt(prefVol.size());
      log.info("Choice = " + prefVol.get(choice));
      return prefVol.get(choice);
    }
    // If the preferred volumes property is not specified, warn the user, then choose randomly from the given list of volumes
    log.warn("No preferred volumes specified.  Defaulting to randomly choosing from instance volumes");
    return choose(options);
  }

  @Override
  public String choose(String[] options) {
    // If table is not specified choose randomly from the given options
    log.info("Choosing without per table properties");
    int choice = random.nextInt(options.length);
    log.info("Choice = " + options[choice]);
    return options[choice];
  }
}
