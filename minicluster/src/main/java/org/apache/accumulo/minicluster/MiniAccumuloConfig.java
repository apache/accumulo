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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.server.util.PortUtils;

/**
 * Holds configuration for {@link org.apache.accumulo.server.mini.MiniAccumuloCluster}. Required configurations must be passed to constructor(s) and all other
 * configurations are optional.
 * 
 * @since 1.5.0
 */
public class MiniAccumuloConfig {
  
  private static final String DEFAULT_INSTANCE_SECRET = "DONTTELL";
  
  private File dir = null;
  private String rootPassword = null;
  private Map<String,String> siteConfig = new HashMap<String,String>();
  private int numTservers = 2;
  private Map<ServerType,Long> memoryConfig = new HashMap<ServerType,Long>();
  
  private boolean jdwpEnabled = false;
  
  private String instanceName = "miniInstance";
  
  private File libDir;
  private File confDir;
  private File zooKeeperDir;
  private File accumuloDir;
  private File logDir;
  private File walogDir;
  
  private Integer zooKeeperPort;
  private long defaultMemorySize = 128 * 1024 * 1024;
  
  private boolean initialized = false;
  
  /**
   * @param dir
   *          An empty or nonexistant directory that Accumulo and Zookeeper can store data in. Creating the directory is left to the user. Java 7, Guava, and
   *          Junit provide methods for creating temporary directories.
   * @param rootPassword
   *          The initial password for the Accumulo root user
   */
  public MiniAccumuloConfig(File dir, String rootPassword) {
    this.dir = dir;
    this.rootPassword = rootPassword;
  }
  
  /**
   * Set directories and fully populate site config
   */
  MiniAccumuloConfig initialize() {
    
    //Sanity checks
    if (this.getDir().exists() && !this.getDir().isDirectory())
      throw new IllegalArgumentException("Must pass in directory, " + this.getDir() + " is a file");

    if (this.getDir().exists() && this.getDir().list().length != 0)
      throw new IllegalArgumentException("Directory " + this.getDir() + " is not empty");
    
    if (!initialized) {
      libDir = new File(dir, "lib");
      confDir = new File(dir, "conf");
      accumuloDir = new File(dir, "accumulo");
      zooKeeperDir = new File(dir, "zookeeper");
      logDir = new File(dir, "logs");
      walogDir = new File(dir, "walogs");
      
      String[] paths = {
          "$ACCUMULO_HOME/lib/.*.jar",
          "$ZOOKEEPER_HOME/zookeeper[^.].*.jar",
          "$HADOOP_PREFIX/[^.].*.jar",
          "$HADOOP_PREFIX/lib/[^.].*.jar",
          "$HADOOP_PREFIX/share/hadoop/common/.*.jar",
          "$HADOOP_PREFIX/share/hadoop/common/lib/.*.jar",
          "$HADOOP_PREFIX/share/hadoop/hdfs/.*.jar",
          "$HADOOP_PREFIX/share/hadoop/mapreduce/.*.jar"
      };
      
      String classpath = StringUtil.join(Arrays.asList(paths), ",");
      
      mergeProp(Property.INSTANCE_DFS_URI.getKey(), "file:///");
      mergeProp(Property.INSTANCE_DFS_DIR.getKey(), accumuloDir.getAbsolutePath());
      mergeProp(Property.INSTANCE_SECRET.getKey(), DEFAULT_INSTANCE_SECRET);
      mergeProp(Property.TSERV_PORTSEARCH.getKey(), "true");
      mergeProp(Property.LOGGER_DIR.getKey(), walogDir.getAbsolutePath());
      mergeProp(Property.TSERV_DATACACHE_SIZE.getKey(), "10M");
      mergeProp(Property.TSERV_INDEXCACHE_SIZE.getKey(), "10M");
      mergeProp(Property.TSERV_MAXMEM.getKey(), "50M");
      mergeProp(Property.TSERV_WALOG_MAX_SIZE.getKey(), "100M");
      mergeProp(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "false");
      mergeProp(Property.TRACE_TOKEN_PROPERTY_PREFIX + ".password", getRootPassword());
      // since there is a small amount of memory, check more frequently for majc... setting may not be needed in 1.5
      mergeProp(Property.TSERV_MAJC_DELAY.getKey(), "3");
      mergeProp(Property.GENERAL_CLASSPATHS.getKey(), classpath);
      mergeProp(Property.GENERAL_DYNAMIC_CLASSPATHS.getKey(), libDir.getAbsolutePath());
      mergePropWithRandomPort(Property.MASTER_CLIENTPORT.getKey());
      mergePropWithRandomPort(Property.TRACE_PORT.getKey());
      mergePropWithRandomPort(Property.TSERV_CLIENTPORT.getKey());
      mergePropWithRandomPort(Property.MONITOR_PORT.getKey());
      
      // zookeeper port should be set explicitly in this class, not just on the site config
      if (zooKeeperPort == null)
        zooKeeperPort = PortUtils.getRandomFreePort();
      siteConfig.put(Property.INSTANCE_ZK_HOST.getKey(), "localhost:" + zooKeeperPort);
      initialized = true;
    }
    return this;
  }
  
  /**
   * Set a given key/value on the site config if it doesn't already exist
   */
  private void mergeProp(String key, String value) {
    if (!siteConfig.containsKey(key)) {
      siteConfig.put(key, value);
    }
  }
  
  /**
   * Sets a given key with a random port for the value on the site config if it doesn't already exist.
   */
  private void mergePropWithRandomPort(String key) {
    if (!siteConfig.containsKey(key)) {
      siteConfig.put(key, PortUtils.getRandomFreePort() + "");
    }
  }
  
  /**
   * Calling this method is optional. If not set, it defaults to two.
   * 
   * @param numTservers
   *          the number of tablet servers that mini accumulo cluster should start
   */
  public MiniAccumuloConfig setNumTservers(int numTservers) {
    if (numTservers < 1)
      throw new IllegalArgumentException("Must have at least one tablet server");
    this.numTservers = numTservers;
    return this;
  }
  
  /**
   * Calling this method is optional. If not set, defaults to 'miniInstance'
   * 
   * @since 1.6.0
   */
  public MiniAccumuloConfig setInstanceName(String instanceName) {
    this.instanceName = instanceName;
    return this;
  }
  
  /**
   * Calling this method is optional. If not set, it defaults to an empty map.
   * 
   * @param siteConfig
   *          key/values that you normally put in accumulo-site.xml can be put here.
   */
  public MiniAccumuloConfig setSiteConfig(Map<String,String> siteConfig) {
    this.siteConfig = new HashMap<String,String>(siteConfig);
    return this;
  }
  
  /**
   * Calling this method is optional. A random port is generated by default
   * 
   * @param zooKeeperPort
   *          A valid (and unused) port to use for the zookeeper
   * 
   * @since 1.6.0
   */
  public MiniAccumuloConfig setZooKeeperPort(int zooKeeperPort) {
    this.zooKeeperPort = zooKeeperPort;
    return this;
  }
  
  /**
   * Sets the amount of memory to use in the master process. Calling this method is optional. Default memory is 128M
   * 
   * @param serverType
   *          the type of server to apply the memory settings
   * @param memory
   *          amount of memory to set
   * 
   * @param memoryUnit
   *          the units for which to apply with the memory size
   * 
   * @since 1.6.0
   */
  public MiniAccumuloConfig setMemory(ServerType serverType, long memory, MemoryUnit memoryUnit) {
    this.memoryConfig.put(serverType, memoryUnit.toBytes(memory));
    return this;
  }
  
  /**
   * Sets the default memory size to use. This value is also used when a ServerType has not been configured explicitly. Calling this method is optional. Default
   * memory is 128M
   * 
   * @param memory
   *          amount of memory to set
   * 
   * @param memoryUnit
   *          the units for which to apply with the memory size
   * 
   * @since 1.6.0
   */
  public MiniAccumuloConfig setDefaultMemory(long memory, MemoryUnit memoryUnit) {
    this.defaultMemorySize = memoryUnit.toBytes(memory);
    return this;
  }
  
  /**
   * @return a copy of the site config
   */
  public Map<String,String> getSiteConfig() {
    return new HashMap<String,String>(siteConfig);
  }
  
  /**
   * @return name of configured instance
   * 
   * @since 1.6.0
   */
  public String getInstanceName() {
    return instanceName;
  }
  
  /**
   * @return The configured zookeeper port
   * 
   * @since 1.6.0
   */
  public int getZooKeeperPort() {
    return zooKeeperPort;
  }
  
  File getLibDir() {
    return libDir;
  }
  
  File getConfDir() {
    return confDir;
  }
  
  File getZooKeeperDir() {
    return zooKeeperDir;
  }
  
  File getAccumuloDir() {
    return accumuloDir;
  }
  
  public File getLogDir() {
    return logDir;
  }
  
  File getWalogDir() {
    return walogDir;
  }
  
  /**
   * @param serverType
   *          get configuration for this server type
   * 
   * @return memory configured in bytes, returns default if this server type is not configured
   * 
   * @since 1.6.0
   */
  public long getMemory(ServerType serverType) {
    return memoryConfig.containsKey(serverType) ? memoryConfig.get(serverType) : defaultMemorySize;
  }
  
  /**
   * @return memory configured in bytes
   * 
   * @since 1.6.0
   */
  public long getDefaultMemory() {
    return defaultMemorySize;
  }
  
  /**
   * @return zookeeper connection string
   * 
   * @since 1.6.0
   */
  public String getZooKeepers() {
    return siteConfig.get(Property.INSTANCE_ZK_HOST.getKey());
  }
  
  /**
   * @return the base directory of the cluster configuration
   */
  public File getDir() {
    return dir;
  }
  
  /**
   * @return the root password of this cluster configuration
   */
  public String getRootPassword() {
    return rootPassword;
  }
  
  /**
   * @return the number of tservers configured for this cluster
   */
  public int getNumTservers() {
    return numTservers;
  }
  
  /**
   * @return is the current configuration in jdwpEnabled mode?
   * 
   * @since 1.6.0
   */
  public boolean isJDWPEnabled() {
    return jdwpEnabled;
  }
  
  /**
   * @param jdwpEnabled
   *          should the processes run remote jdwpEnabled servers?
   * @return the current instance
   * 
   * @since 1.6.0
   */
  public MiniAccumuloConfig setJDWPEnabled(boolean jdwpEnabled) {
    this.jdwpEnabled = jdwpEnabled;
    return this;
  }
  
}
