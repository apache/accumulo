package org.apache.accumulo.miniclusterImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.lock.ServiceLockData;

public class ClusterServerConfiguration {

  private final Map<String,Integer> compactors;
  private final Map<String,Integer> sservers;
  private final Map<String,Integer> tservers;
  
  /**
   * Creates the default configuration with 1 each of
   * Compactor and ScanServer and 2 TabletServers in the default
   * resource group
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
    compactors.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numCompactors);
    sservers = new HashMap<>();
    sservers.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numSServers);
    tservers = new HashMap<>();
    tservers.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numTServers);
  }
  
  public void setNumDefaultCompactors(int numCompactors) {
    compactors.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numCompactors);
  }
  
  public void setNumDefaultScanServers(int numSServers) {
    sservers.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numSServers);
  }

  public void setNumDefaultTabletServers(int numTServers) {
    tservers.put(ServiceLockData.ServiceDescriptor.DEFAULT_GROUP_NAME, numTServers);
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

}
