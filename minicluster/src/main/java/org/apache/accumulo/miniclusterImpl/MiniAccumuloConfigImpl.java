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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.HadoopCredentialProvider;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds configuration for {@link MiniAccumuloClusterImpl}. Required configurations must be passed
 * to constructor(s) and all other configurations are optional.
 *
 * @since 1.6.0
 */
public class MiniAccumuloConfigImpl {
  private static final Logger log = LoggerFactory.getLogger(MiniAccumuloConfigImpl.class);
  private static final String DEFAULT_INSTANCE_SECRET = "DONTTELL";

  private File dir = null;
  private String rootPassword = null;
  private Map<String,String> siteConfig = new HashMap<>();
  private Map<String,String> configuredSiteConig = new HashMap<>();
  private Map<String,String> clientProps = new HashMap<>();
  private int numTservers = 2;
  private int numScanServers = 0;
  private int numCompactors = 1;
  private Map<ServerType,Long> memoryConfig = new HashMap<>();
  private boolean jdwpEnabled = false;
  private Map<String,String> systemProperties = new HashMap<>();

  private String instanceName = "miniInstance";
  private String rootUserName = "root";

  private File libDir;
  private File libExtDir;
  private File confDir;
  private File hadoopConfDir = null;
  private File zooKeeperDir;
  private File accumuloDir;
  private File logDir;

  private int zooKeeperPort = 0;
  private int configuredZooKeeperPort = 0;
  private long zooKeeperStartupTime = 20_000;
  private String existingZooKeepers;

  private long defaultMemorySize = 256 * 1024 * 1024;

  private boolean initialized = false;

  // TODO Nuke existingInstance and push it over to StandaloneAccumuloCluster
  private Boolean existingInstance = null;

  private boolean useMiniDFS = false;

  private boolean useCredentialProvider = false;

  private String[] classpathItems = null;

  private String[] nativePathItems = null;

  // These are only used on top of existing instances
  private Configuration hadoopConf;
  private SiteConfiguration accumuloConf;

  /**
   * @param dir An empty or nonexistent directory that Accumulo and Zookeeper can store data in.
   *        Creating the directory is left to the user. Java 7, Guava, and Junit provide methods for
   *        creating temporary directories.
   * @param rootPassword The initial password for the Accumulo root user
   */
  public MiniAccumuloConfigImpl(File dir, String rootPassword) {
    this.dir = dir;
    this.rootPassword = rootPassword;
  }

  /**
   * Set directories and fully populate site config
   *
   * @return this
   */
  MiniAccumuloConfigImpl initialize() {

    // Sanity checks
    if (this.getDir().exists() && !this.getDir().isDirectory()) {
      throw new IllegalArgumentException("Must pass in directory, " + this.getDir() + " is a file");
    }

    if (this.getDir().exists()) {
      String[] children = this.getDir().list();
      if (children != null && children.length != 0) {
        throw new IllegalArgumentException("Directory " + this.getDir() + " is not empty");
      }
    }

    if (!initialized) {
      libDir = new File(dir, "lib");
      libExtDir = new File(libDir, "ext");
      confDir = new File(dir, "conf");
      accumuloDir = new File(dir, "accumulo");
      zooKeeperDir = new File(dir, "zookeeper");
      logDir = new File(dir, "logs");

      // Never want to override these if an existing instance, which may be using the defaults
      if (existingInstance == null || !existingInstance) {
        existingInstance = false;
        mergeProp(Property.INSTANCE_VOLUMES.getKey(), "file://" + accumuloDir.getAbsolutePath());
        mergeProp(Property.INSTANCE_SECRET.getKey(), DEFAULT_INSTANCE_SECRET);
      }

      mergeProp(Property.TSERV_PORTSEARCH.getKey(), "true");
      mergeProp(Property.TSERV_DATACACHE_SIZE.getKey(), "10M");
      mergeProp(Property.TSERV_INDEXCACHE_SIZE.getKey(), "10M");
      mergeProp(Property.TSERV_SUMMARYCACHE_SIZE.getKey(), "10M");
      mergeProp(Property.TSERV_MAXMEM.getKey(), "40M");
      mergeProp(Property.TSERV_WAL_MAX_SIZE.getKey(), "100M");
      mergeProp(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "false");
      // since there is a small amount of memory, check more frequently for majc... setting may not
      // be needed in 1.5
      mergeProp(Property.TSERV_MAJC_DELAY.getKey(), "3");
      @SuppressWarnings("deprecation")
      Property generalClasspaths = Property.GENERAL_CLASSPATHS;
      mergeProp(generalClasspaths.getKey(), libDir.getAbsolutePath() + "/[^.].*[.]jar");
      @SuppressWarnings("deprecation")
      Property generalDynamicClasspaths = Property.GENERAL_DYNAMIC_CLASSPATHS;
      mergeProp(generalDynamicClasspaths.getKey(), libExtDir.getAbsolutePath() + "/[^.].*[.]jar");
      mergeProp(Property.GC_CYCLE_DELAY.getKey(), "4s");
      mergeProp(Property.GC_CYCLE_START.getKey(), "0s");
      mergePropWithRandomPort(Property.MANAGER_CLIENTPORT.getKey());
      mergePropWithRandomPort(Property.TSERV_CLIENTPORT.getKey());
      mergePropWithRandomPort(Property.MONITOR_PORT.getKey());
      mergePropWithRandomPort(Property.GC_PORT.getKey());

      if (isUseCredentialProvider()) {
        updateConfigForCredentialProvider();
      }

      if (existingInstance == null || !existingInstance) {
        existingInstance = false;
        String zkHost;
        if (useExistingZooKeepers()) {
          zkHost = existingZooKeepers;
        } else {
          // zookeeper port should be set explicitly in this class, not just on the site config
          if (zooKeeperPort == 0) {
            zooKeeperPort = PortUtils.getRandomFreePort();
          }

          zkHost = "localhost:" + zooKeeperPort;
        }
        siteConfig.put(Property.INSTANCE_ZK_HOST.getKey(), zkHost);
      }
      initialized = true;
    }
    return this;
  }

  private void updateConfigForCredentialProvider() {
    String cpPaths = siteConfig.get(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
    if (cpPaths != null
        && !Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getDefaultValue().equals(cpPaths)) {
      // Already configured
      return;
    }

    File keystoreFile = new File(getConfDir(), "credential-provider.jks");
    String keystoreUri = "jceks://file" + keystoreFile.getAbsolutePath();
    Configuration conf = getHadoopConfiguration();
    HadoopCredentialProvider.setPath(conf, keystoreUri);

    // Set the URI on the siteCfg
    siteConfig.put(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), keystoreUri);

    Iterator<Entry<String,String>> entries = siteConfig.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<String,String> entry = entries.next();

      // Not a @Sensitive Property, ignore it
      if (!Property.isSensitive(entry.getKey())) {
        continue;
      }

      // Add the @Sensitive Property to the CredentialProvider
      try {
        HadoopCredentialProvider.createEntry(conf, entry.getKey(), entry.getValue().toCharArray());
      } catch (IOException e) {
        log.warn("Attempted to add " + entry.getKey() + " to CredentialProvider but failed", e);
        continue;
      }

      // Only remove it from the siteCfg if we succeeded in adding it to the CredentialProvider
      entries.remove();
    }
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
   * Sets a given key with a random port for the value on the site config if it doesn't already
   * exist.
   */
  private void mergePropWithRandomPort(String key) {
    if (!siteConfig.containsKey(key)) {
      siteConfig.put(key, "0");
    }
  }

  /**
   * Calling this method is optional. If not set, it defaults to two.
   *
   * @param numTservers the number of tablet servers that mini accumulo cluster should start
   */
  public MiniAccumuloConfigImpl setNumTservers(int numTservers) {
    if (numTservers < 1) {
      throw new IllegalArgumentException("Must have at least one tablet server");
    }
    this.numTservers = numTservers;
    return this;
  }

  /**
   * Calling this method is optional. If not set, it defaults to two.
   *
   * @param numScanServers the number of tablet servers that mini accumulo cluster should start
   */
  public MiniAccumuloConfigImpl setNumScanServers(int numScanServers) {
    if (numScanServers < 0) {
      throw new IllegalArgumentException("Must have zero or more scan servers");
    }
    this.numScanServers = numScanServers;
    return this;
  }

  /**
   * Calling this method is optional. If not set, defaults to 'miniInstance'
   *
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setInstanceName(String instanceName) {
    this.instanceName = instanceName;
    return this;
  }

  /**
   * Calling this method is optional. If not set, it defaults to an empty map.
   *
   * @param siteConfig key/values that you normally put in accumulo.properties can be put here.
   */
  public MiniAccumuloConfigImpl setSiteConfig(Map<String,String> siteConfig) {
    if (existingInstance != null && existingInstance) {
      throw new UnsupportedOperationException(
          "Cannot set set config info when using an existing instance.");
    }

    this.existingInstance = Boolean.FALSE;

    return _setSiteConfig(siteConfig);
  }

  public MiniAccumuloConfigImpl setClientProps(Map<String,String> clientProps) {
    if (existingInstance != null && existingInstance) {
      throw new UnsupportedOperationException(
          "Cannot set zookeeper info when using an existing instance.");
    }
    this.existingInstance = Boolean.FALSE;
    this.clientProps = clientProps;
    return this;
  }

  private MiniAccumuloConfigImpl _setSiteConfig(Map<String,String> siteConfig) {
    this.siteConfig = new HashMap<>(siteConfig);
    this.configuredSiteConig = new HashMap<>(siteConfig);
    return this;
  }

  /**
   * Calling this method is optional. A random port is generated by default
   *
   * @param zooKeeperPort A valid (and unused) port to use for the zookeeper
   *
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setZooKeeperPort(int zooKeeperPort) {
    if (existingInstance != null && existingInstance) {
      throw new UnsupportedOperationException(
          "Cannot set zookeeper info when using an existing instance.");
    }

    this.existingInstance = Boolean.FALSE;

    this.configuredZooKeeperPort = zooKeeperPort;
    this.zooKeeperPort = zooKeeperPort;
    return this;
  }

  /**
   * Configure the time to wait for ZooKeeper to startup. Calling this method is optional. The
   * default is 20000 milliseconds
   *
   * @param zooKeeperStartupTime Time to wait for ZooKeeper to startup, in milliseconds
   *
   * @since 1.6.1
   */
  public MiniAccumuloConfigImpl setZooKeeperStartupTime(long zooKeeperStartupTime) {
    if (existingInstance != null && existingInstance) {
      throw new UnsupportedOperationException(
          "Cannot set zookeeper info when using an existing instance.");
    }

    this.existingInstance = Boolean.FALSE;

    this.zooKeeperStartupTime = zooKeeperStartupTime;
    return this;
  }

  /**
   * Configure an existing ZooKeeper instance to use. Calling this method is optional. If not set, a
   * new ZooKeeper instance is created.
   *
   * @param existingZooKeepers Connection string for a already-running ZooKeeper instance. A null
   *        value will turn off this feature.
   *
   * @since 1.8.0
   */
  public MiniAccumuloConfigImpl setExistingZooKeepers(String existingZooKeepers) {
    this.existingZooKeepers = existingZooKeepers;
    return this;
  }

  /**
   * Sets the amount of memory to use in the specified process. Calling this method is optional.
   * Default memory is 256M
   *
   * @param serverType the type of server to apply the memory settings
   * @param memory amount of memory to set
   *
   * @param memoryUnit the units for which to apply with the memory size
   *
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setMemory(ServerType serverType, long memory,
      MemoryUnit memoryUnit) {
    this.memoryConfig.put(serverType, memoryUnit.toBytes(memory));
    return this;
  }

  /**
   * Sets the default memory size to use. This value is also used when a ServerType has not been
   * configured explicitly. Calling this method is optional. Default memory is 256M
   *
   * @param memory amount of memory to set
   *
   * @param memoryUnit the units for which to apply with the memory size
   *
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setDefaultMemory(long memory, MemoryUnit memoryUnit) {
    this.defaultMemorySize = memoryUnit.toBytes(memory);
    return this;
  }

  /**
   * @return a copy of the site config
   */
  public Map<String,String> getSiteConfig() {
    return new HashMap<>(siteConfig);
  }

  /**
   * @return a copy of client props
   */
  public Map<String,String> getClientProps() {
    return new HashMap<>(clientProps);
  }

  public Map<String,String> getConfiguredSiteConfig() {
    return new HashMap<>(configuredSiteConig);
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

  public int getConfiguredZooKeeperPort() {
    return configuredZooKeeperPort;
  }

  public long getZooKeeperStartupTime() {
    return zooKeeperStartupTime;
  }

  public String getExistingZooKeepers() {
    return existingZooKeepers;
  }

  public boolean useExistingZooKeepers() {
    return existingZooKeepers != null && !existingZooKeepers.isEmpty();
  }

  File getLibDir() {
    return libDir;
  }

  File getLibExtDir() {
    return libExtDir;
  }

  public File getConfDir() {
    return confDir;
  }

  File getZooKeeperDir() {
    return zooKeeperDir;
  }

  public File getAccumuloDir() {
    return accumuloDir;
  }

  public File getLogDir() {
    return logDir;
  }

  /**
   * @param serverType get configuration for this server type
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
   * @return the number of scan servers configured for this cluster
   */
  public int getNumScanServers() {
    return numScanServers;
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
   * @param jdwpEnabled should the processes run remote jdwpEnabled servers?
   * @return the current instance
   *
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setJDWPEnabled(boolean jdwpEnabled) {
    this.jdwpEnabled = jdwpEnabled;
    return this;
  }

  public boolean useMiniDFS() {
    return useMiniDFS;
  }

  /**
   * Configures this cluster to use miniDFS instead of the local {@link FileSystem}. Using this
   * feature will not allow you to re-start {@link MiniAccumuloCluster} by calling
   * {@link MiniAccumuloCluster#start()} after {@link MiniAccumuloCluster#stop()}, because the
   * underlying miniDFS cannot be restarted.
   */
  public void useMiniDFS(boolean useMiniDFS) {
    this.useMiniDFS = useMiniDFS;
  }

  public File getAccumuloPropsFile() {
    return new File(getConfDir(), "accumulo.properties");
  }

  /**
   * @return location of accumulo-client.properties file for connecting to this mini cluster
   */
  public File getClientPropsFile() {
    return new File(getConfDir(), "accumulo-client.properties");
  }

  /**
   * sets system properties set for service processes
   *
   * @since 1.6.0
   */
  public void setSystemProperties(Map<String,String> systemProperties) {
    this.systemProperties = new HashMap<>(systemProperties);
  }

  /**
   * @return a copy of the system properties for service processes
   *
   * @since 1.6.0
   */
  public Map<String,String> getSystemProperties() {
    return new HashMap<>(systemProperties);
  }

  /**
   * Gets the classpath elements to use when spawning processes.
   *
   * @return the classpathItems, if set
   *
   * @since 1.6.0
   */
  public String[] getClasspathItems() {
    return classpathItems;
  }

  /**
   * Sets the classpath elements to use when spawning processes.
   *
   * @param classpathItems the classpathItems to set
   * @since 1.6.0
   */
  public void setClasspathItems(String... classpathItems) {
    this.classpathItems = classpathItems;
  }

  /**
   * @return the paths to use for loading native libraries
   *
   * @since 1.6.0
   */
  public String[] getNativeLibPaths() {
    return this.nativePathItems == null ? new String[0] : this.nativePathItems;
  }

  /**
   * Sets the path for processes to use for loading native libraries
   *
   * @param nativePathItems the nativePathItems to set
   * @since 1.6.0
   */
  public MiniAccumuloConfigImpl setNativeLibPaths(String... nativePathItems) {
    this.nativePathItems = nativePathItems;
    return this;
  }

  /**
   * Sets arbitrary configuration properties.
   *
   * @since 1.6.0
   */
  public void setProperty(Property p, String value) {
    this.siteConfig.put(p.getKey(), value);
  }

  public void setClientProperty(ClientProperty property, String value) {
    setClientProperty(property.getKey(), value);
  }

  public void setClientProperty(String key, String value) {
    this.clientProps.put(key, value);
  }

  /**
   * Sets arbitrary configuration properties.
   *
   * @since 2.0.0
   */
  public void setProperty(String p, String value) {
    this.siteConfig.put(p, value);
  }

  /**
   * @return the useCredentialProvider
   */
  public boolean isUseCredentialProvider() {
    return useCredentialProvider;
  }

  /**
   * @param useCredentialProvider the useCredentialProvider to set
   */
  public void setUseCredentialProvider(boolean useCredentialProvider) {
    this.useCredentialProvider = useCredentialProvider;
  }

  /**
   * Informs MAC that it's running against an existing accumulo instance. It is assumed that it's
   * already initialized and hdfs/zookeeper are already running.
   *
   * @param accumuloProps a File representation of the accumulo.properties file for the instance
   *        being run
   * @param hadoopConfDir a File representation of the hadoop configuration directory containing
   *        core-site.xml and hdfs-site.xml
   *
   * @return MiniAccumuloConfigImpl which uses an existing accumulo configuration
   *
   * @since 1.6.2
   *
   * @throws IOException when there are issues converting the provided Files to URLs
   */
  public MiniAccumuloConfigImpl useExistingInstance(File accumuloProps, File hadoopConfDir)
      throws IOException {
    if (existingInstance != null && !existingInstance) {
      throw new UnsupportedOperationException(
          "Cannot set to useExistingInstance after specifying config/zookeeper");
    }

    this.existingInstance = Boolean.TRUE;

    System.setProperty("accumulo.properties", "accumulo.properties");
    this.hadoopConfDir = hadoopConfDir;
    hadoopConf = new Configuration(false);
    accumuloConf = SiteConfiguration.fromFile(accumuloProps).build();
    File coreSite = new File(hadoopConfDir, "core-site.xml");
    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");

    try {
      hadoopConf.addResource(coreSite.toURI().toURL());
      hadoopConf.addResource(hdfsSite.toURI().toURL());
    } catch (MalformedURLException e1) {
      throw e1;
    }

    Map<String,String> siteConfigMap = new HashMap<>();
    for (Entry<String,String> e : accumuloConf) {
      siteConfigMap.put(e.getKey(), e.getValue());
    }
    _setSiteConfig(siteConfigMap);

    return this;
  }

  /**
   * @return MAC should run assuming it's configured for an initialized accumulo instance
   *
   * @since 1.6.2
   */
  public boolean useExistingInstance() {
    return existingInstance != null && existingInstance;
  }

  /**
   * @return hadoop configuration directory being used
   *
   * @since 1.6.2
   */
  public File getHadoopConfDir() {
    return this.hadoopConfDir;
  }

  /**
   * @return accumulo Configuration being used
   *
   * @since 1.6.2
   */
  public AccumuloConfiguration getAccumuloConfiguration() {
    return accumuloConf;
  }

  /**
   * @return hadoop Configuration being used
   *
   * @since 1.6.2
   */
  public Configuration getHadoopConfiguration() {
    return hadoopConf;
  }

  /**
   * @return the default Accumulo "superuser"
   * @since 1.7.0
   */
  public String getRootUserName() {
    return rootUserName;
  }

  /**
   * Sets the default Accumulo "superuser".
   *
   * @param rootUserName The name of the user to create with administrative permissions during
   *        initialization
   * @since 1.7.0
   */
  public void setRootUserName(String rootUserName) {
    this.rootUserName = rootUserName;
  }

  /**
   * @return number of Compactors
   * @since 2.1.0
   */
  public int getNumCompactors() {
    return numCompactors;
  }

  /**
   * Set number of Compactors
   *
   * @param numCompactors number of compactors
   * @since 2.1.0
   */
  public void setNumCompactors(int numCompactors) {
    this.numCompactors = numCompactors;
  }
}
