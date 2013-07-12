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
package org.apache.accumulo.minicluster;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 */
public class MiniAccumuloInstance extends ZooKeeperInstance {
  
  /**
   * Construct an {@link Instance} entry point to Accumulo using a {@link MiniAccumuloCluster} directory
   */
  public MiniAccumuloInstance(String instanceName, File directory) {
    super(instanceName, getZooKeepersFromDir(directory));
  }
  
  private static String getZooKeepersFromDir(File directory) {
    if (!directory.isDirectory())
      throw new IllegalArgumentException("Not a directory " + directory.getPath());
    File configFile = new File(new File(directory, "conf"), "accumulo-site.xml");
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(configFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new IllegalStateException("Missing file: " + configFile.getPath());
    }
    return conf.get(Property.INSTANCE_ZK_HOST.getKey());
  }
}
