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
package org.apache.accumulo.server.conf;

import java.security.SecurityPermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class ServerConfiguration {
  
  private static Map<String,TableConfiguration> tableInstances = new HashMap<String,TableConfiguration>(1);
  private static SecurityPermission CONFIGURATION_PERMISSION = new SecurityPermission("configurationPermission");
  
  public static synchronized SiteConfiguration getSiteConfiguration() {
    checkPermissions();
    return SiteConfiguration.getInstance(getDefaultConfiguration());
  }
  
  private static void checkPermissions() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(CONFIGURATION_PERMISSION);
    }
  }
  
  private static synchronized ZooConfiguration getZooConfiguration() {
    checkPermissions();
    return ZooConfiguration.getInstance(getSiteConfiguration());
  }
  
  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    checkPermissions();
    return DefaultConfiguration.getInstance();
  }
  
  public static synchronized AccumuloConfiguration getSystemConfiguration() {
    return getZooConfiguration();
  }
  
  public static TableConfiguration getTableConfiguration(String instanceId, String tableId) {
    checkPermissions();
    synchronized (tableInstances) {
      TableConfiguration conf = tableInstances.get(tableId);
      if (conf == null) {
        conf = new TableConfiguration(instanceId, tableId, getSystemConfiguration());
        ConfigSanityCheck.validate(conf);
        tableInstances.put(tableId, conf);
      }
      return conf;
    }
  }
  
  public static TableConfiguration getTableConfiguration(String tableId) {
    return getTableConfiguration(HdfsZooInstance.getInstance().getInstanceID(), tableId);
  }
  
  static void removeTableIdInstance(String tableId) {
    synchronized (tableInstances) {
      tableInstances.remove(tableId);
    }
  }
  
  static void expireAllTableObservers() {
    synchronized (tableInstances) {
      for (Entry<String,TableConfiguration> entry : tableInstances.entrySet()) {
        entry.getValue().expireAllObservers();
      }
    }
  }
}
