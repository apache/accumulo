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
package org.apache.accumulo.miniclusterImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.Constants;

public class ClusterServerConfiguration {

  private final Map<String,Integer> compactors;
  private final Map<String,Integer> sservers;
  private final Map<String,Integer> tservers;

  /**
   * Creates the default configuration with 1 each of Compactor and ScanServer and 2 TabletServers
   * in the default resource group
   */
  public ClusterServerConfiguration() {
    this(1, 1, 2);
  }

  /**
   * Creates the default Configuration using the parameters
   *
   * @param numCompactors number of compactors in the default resource group
   * @param numSServers number of scan servers in the default resource group
   * @param numTServers number of tablet servers in the default resource group
   */
  public ClusterServerConfiguration(int numCompactors, int numSServers, int numTServers) {
    compactors = new HashMap<>();
    compactors.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numCompactors);
    sservers = new HashMap<>();
    sservers.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numSServers);
    tservers = new HashMap<>();
    tservers.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numTServers);
  }

  public void setNumDefaultCompactors(int numCompactors) {
    compactors.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numCompactors);
  }

  public void setNumDefaultScanServers(int numSServers) {
    sservers.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numSServers);
  }

  public void setNumDefaultTabletServers(int numTServers) {
    tservers.put(Constants.DEFAULT_RESOURCE_GROUP_NAME, numTServers);
  }

  public void addCompactorResourceGroup(String resourceGroupName, int numCompactors) {
    compactors.put(resourceGroupName, numCompactors);
  }

  public void addScanServerResourceGroup(String resourceGroupName, int numScanServers) {
    sservers.put(resourceGroupName, numScanServers);
  }

  public void addTabletServerResourceGroup(String resourceGroupName, int numTabletServers) {
    tservers.put(resourceGroupName, numTabletServers);
  }

  public Map<String,Integer> getCompactorConfiguration() {
    return Collections.unmodifiableMap(compactors);
  }

  public Map<String,Integer> getScanServerConfiguration() {
    return Collections.unmodifiableMap(sservers);
  }

  public Map<String,Integer> getTabletServerConfiguration() {
    return Collections.unmodifiableMap(tservers);
  }

  public void clearCompactorResourceGroups() {
    Iterator<String> iter = compactors.keySet().iterator();
    while (iter.hasNext()) {
      String resourceGroup = iter.next();
      if (!resourceGroup.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME)) {
        iter.remove();
      }
    }
  }

  public void clearTServerResourceGroups() {
    Iterator<String> iter = tservers.keySet().iterator();
    while (iter.hasNext()) {
      String resourceGroup = iter.next();
      if (!resourceGroup.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME)) {
        iter.remove();
      }
    }
  }

}
