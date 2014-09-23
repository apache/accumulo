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
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.log4j.Logger;

public class GeneralVolumeChooser implements VolumeChooser {
  private static final Logger log = Logger.getLogger(GeneralVolumeChooser.class);

  public GeneralVolumeChooser() {}

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    String clazzName = new String();
    try {
      // Get the current table's properties, and find the chooser property
      PropertyFilter filter = new AllFilter();
      Map<String,String> props = new java.util.HashMap<String,String>();
      env.getProperties(props, filter);

      clazzName = props.get("table.custom.chooser");
      log.info("TableID: " + env.getTableId() + " ClassName: " + clazzName);

      // Load the correct chooser and create an instance of it
      Class<? extends VolumeChooser> clazz = AccumuloVFSClassLoader.loadClass(clazzName, VolumeChooser.class);
      VolumeChooser instance = clazz.newInstance();
      // Choose the volume based using the created chooser
      return instance.choose(env, options);
    } catch (Exception e) {
      // If an exception occurs, first write a warning and then use a default RandomVolumeChooser to choose from the given options
      log.warn(e, e);
      return new RandomVolumeChooser().choose(options);
    }
  }

  @Override
  public String choose(String[] options) {
    return new RandomVolumeChooser().choose(options);
  }

}
