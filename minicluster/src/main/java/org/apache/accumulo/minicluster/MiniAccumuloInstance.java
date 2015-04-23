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
import java.io.FileNotFoundException;
import java.net.MalformedURLException;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @since 1.6.0
 */
public class MiniAccumuloInstance extends ZooKeeperInstance {

  /**
   * Construct an {@link Instance} entry point to Accumulo using a {@link MiniAccumuloCluster} directory
   */
  public MiniAccumuloInstance(String instanceName, File directory) throws FileNotFoundException {
    super(new ClientConfiguration(getConfigProperties(directory)).withInstance(instanceName).withZkHosts(getZooKeepersFromDir(directory)));
  }

  public static PropertiesConfiguration getConfigProperties(File directory) {
    try {
      return new PropertiesConfiguration(new File(new File(directory, "conf"), "client.conf"));
    } catch (ConfigurationException e) {
      // this should never happen since we wrote the config file ourselves
      throw new IllegalArgumentException(e);
    }
  }

  private static String getZooKeepersFromDir(File directory) throws FileNotFoundException {
    if (!directory.isDirectory())
      throw new IllegalArgumentException("Not a directory " + directory.getPath());
    File configFile = new File(new File(directory, "conf"), "accumulo-site.xml");
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(configFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new FileNotFoundException("Missing file: " + configFile.getPath());
    }
    return conf.get(Property.INSTANCE_ZK_HOST.getKey());
  }
}
