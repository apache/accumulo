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
package org.apache.accumulo.core.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * A configuration object.
 */
public abstract class AccumuloConfiguration implements Iterable<Entry<String,String>> {

  /**
   * A filter for properties, based on key.
   *
   * @deprecated since 1.7.0; use {@link Predicate} instead.
   */
  @Deprecated
  public interface PropertyFilter {
    /**
     * Determines whether to accept a property based on its key.
     *
     * @param key
     *          property key
     * @return true to accept property (pass filter)
     */
    boolean accept(String key);
  }

  /**
   * A filter that accepts properties whose keys are an exact match.
   */
  public static class MatchFilter implements Predicate<String> {

    private String match;

    /**
     * Creates a new filter.
     *
     * @param match
     *          prefix of property keys to accept
     */
    public MatchFilter(String match) {
      this.match = match;
    }

    @Override
    public boolean apply(String key) {
      return Objects.equals(match, key);
    }
  }

  /**
   * A filter that accepts properties whose keys begin with a prefix.
   */
  public static class PrefixFilter implements Predicate<String> {

    private String prefix;

    /**
     * Creates a new filter.
     *
     * @param prefix
     *          prefix of property keys to accept
     */
    public PrefixFilter(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public boolean apply(String key) {
      return key.startsWith(prefix);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(AccumuloConfiguration.class);

  /**
   * Gets a property value from this configuration.
   *
   * <p>
   * Note: this is inefficient, but convenient on occasion. For retrieving multiple properties, use {@link #getProperties(Map, Predicate)} with a custom filter.
   *
   * @param property
   *          property to get
   * @return property value
   */
  public String get(String property) {
    Map<String,String> propMap = new HashMap<String,String>(1);
    getProperties(propMap, new MatchFilter(property));
    return propMap.get(property);
  }

  /**
   * Gets a property value from this configuration.
   *
   * @param property
   *          property to get
   * @return property value
   */
  public abstract String get(Property property);

  /**
   * Returns property key/value pairs in this configuration. The pairs include those defined in this configuration which pass the given filter, and those
   * supplied from the parent configuration which are not included from here.
   *
   * @param props
   *          properties object to populate
   * @param filter
   *          filter for accepting properties from this configuration
   */
  public abstract void getProperties(Map<String,String> props, Predicate<String> filter);

  /**
   * Returns an iterator over property key/value pairs in this configuration. Some implementations may elect to omit properties.
   *
   * @return iterator over properties
   */
  @Override
  public Iterator<Entry<String,String>> iterator() {
    Predicate<String> all = Predicates.alwaysTrue();
    TreeMap<String,String> entries = new TreeMap<String,String>();
    getProperties(entries, all);
    return entries.entrySet().iterator();
  }

  private void checkType(Property property, PropertyType type) {
    if (!property.getType().equals(type)) {
      String msg = "Configuration method intended for type " + type + " called with a " + property.getType() + " argument (" + property.getKey() + ")";
      IllegalArgumentException err = new IllegalArgumentException(msg);
      log.error(msg, err);
      throw err;
    }
  }

  /**
   * Gets all properties under the given prefix in this configuration.
   *
   * @param property
   *          prefix property, must be of type PropertyType.PREFIX
   * @return a map of property keys to values
   * @throws IllegalArgumentException
   *           if property is not a prefix
   */
  public Map<String,String> getAllPropertiesWithPrefix(Property property) {
    checkType(property, PropertyType.PREFIX);

    Map<String,String> propMap = new HashMap<String,String>();
    getProperties(propMap, new PrefixFilter(property.getKey()));
    return propMap;
  }

  /**
   * Gets a property of type {@link PropertyType#MEMORY}, interpreting the value properly.
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see #getMemoryInBytes(String)
   */
  public long getMemoryInBytes(Property property) {
    checkType(property, PropertyType.MEMORY);

    String memString = get(property);
    return getMemoryInBytes(memString);
  }

  /**
   * Interprets a string specifying a memory size. A memory size is specified as a long integer followed by an optional B (bytes), K (KB), M (MB), or G (GB).
   *
   * @param str
   *          string value
   * @return interpreted memory size
   */
  static public long getMemoryInBytes(String str) {
    char lastChar = str.charAt(str.length() - 1);

    if (lastChar == 'b') {
      log.warn("The 'b' in " + str + " is being considered as bytes. " + "Setting memory by bits is not supported");
    }
    try {
      int multiplier;
      switch (Character.toUpperCase(lastChar)) {
        case 'G':
          multiplier = 30;
          break;
        case 'M':
          multiplier = 20;
          break;
        case 'K':
          multiplier = 10;
          break;
        case 'B':
          multiplier = 0;
          break;
        default:
          return Long.parseLong(str);
      }
      return Long.parseLong(str.substring(0, str.length() - 1)) << multiplier;
    } catch (Exception ex) {
      throw new IllegalArgumentException("The value '" + str + "' is not a valid memory setting. A valid value would a number "
          + "possibily followed by an optional 'G', 'M', 'K', or 'B'.");
    }
  }

  /**
   * Gets a property of type {@link PropertyType#TIMEDURATION}, interpreting the value properly.
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see #getTimeInMillis(String)
   */
  public long getTimeInMillis(Property property) {
    checkType(property, PropertyType.TIMEDURATION);

    return getTimeInMillis(get(property));
  }

  /**
   * Interprets a string specifying a time duration. A time duration is specified as a long integer followed by an optional d (days), h (hours), m (minutes), s
   * (seconds), or ms (milliseconds). A value without a unit is interpreted as seconds.
   *
   * @param str
   *          string value
   * @return interpreted time duration in milliseconds
   */
  public static long getTimeInMillis(String str) {
    TimeUnit timeUnit;
    int unitsLen = 1;
    switch (str.charAt(str.length() - 1)) {
      case 'd':
        timeUnit = TimeUnit.DAYS;
        break;
      case 'h':
        timeUnit = TimeUnit.HOURS;
        break;
      case 'm':
        timeUnit = TimeUnit.MINUTES;
        break;
      case 's':
        timeUnit = TimeUnit.SECONDS;
        if (str.endsWith("ms")) {
          timeUnit = TimeUnit.MILLISECONDS;
          unitsLen = 2;
        }
        break;
      default:
        timeUnit = TimeUnit.SECONDS;
        unitsLen = 0;
        break;
    }
    return timeUnit.toMillis(Long.parseLong(str.substring(0, str.length() - unitsLen)));
  }

  /**
   * Gets a property of type {@link PropertyType#BOOLEAN}, interpreting the value properly (using <code>Boolean.parseBoolean()</code>).
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   */
  public boolean getBoolean(Property property) {
    checkType(property, PropertyType.BOOLEAN);
    return Boolean.parseBoolean(get(property));
  }

  /**
   * Gets a property of type {@link PropertyType#FRACTION}, interpreting the value properly.
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see #getFraction(String)
   */
  public double getFraction(Property property) {
    checkType(property, PropertyType.FRACTION);

    return getFraction(get(property));
  }

  /**
   * Interprets a string specifying a fraction. A fraction is specified as a double. An optional % at the end signifies a percentage.
   *
   * @param str
   *          string value
   * @return interpreted fraction as a decimal value
   */
  public double getFraction(String str) {
    if (str.length() > 0 && str.charAt(str.length() - 1) == '%')
      return Double.parseDouble(str.substring(0, str.length() - 1)) / 100.0;
    return Double.parseDouble(str);
  }

  /**
   * Gets a property of type {@link PropertyType#PORT}, interpreting the value properly (as an integer within the range of non-privileged ports).
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see #getTimeInMillis(String)
   */
  public int getPort(Property property) {
    checkType(property, PropertyType.PORT);

    String portString = get(property);
    int port = Integer.parseInt(portString);
    if (port != 0) {
      if (port < 1024 || port > 65535) {
        log.error("Invalid port number " + port + "; Using default " + property.getDefaultValue());
        port = Integer.parseInt(property.getDefaultValue());
      }
    }
    return port;
  }

  /**
   * Gets a property of type {@link PropertyType#COUNT}, interpreting the value properly (as an integer).
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see #getTimeInMillis(String)
   */
  public int getCount(Property property) {
    checkType(property, PropertyType.COUNT);

    String countString = get(property);
    return Integer.parseInt(countString);
  }

  /**
   * Gets a property of type {@link PropertyType#PATH}, interpreting the value properly, replacing supported environment variables.
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   * @see Constants#PATH_PROPERTY_ENV_VARS
   */
  public String getPath(Property property) {
    checkType(property, PropertyType.PATH);

    String pathString = get(property);
    if (pathString == null)
      return null;

    for (String replaceableEnvVar : Constants.PATH_PROPERTY_ENV_VARS) {
      String envValue = System.getenv(replaceableEnvVar);
      if (envValue != null)
        pathString = pathString.replace("$" + replaceableEnvVar, envValue);
    }

    return pathString;
  }

  /**
   * Gets the default configuration.
   *
   * @return default configuration
   * @see DefaultConfiguration#getInstance()
   */
  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    return DefaultConfiguration.getInstance();
  }

  /**
   * Gets the configuration specific to a table.
   *
   * @param conn
   *          connector (used to find table name)
   * @param tableId
   *          table ID
   * @return configuration containing table properties
   * @throws TableNotFoundException
   *           if the table is not found
   * @throws AccumuloException
   *           if there is a problem communicating to Accumulo
   */
  public static AccumuloConfiguration getTableConfiguration(Connector conn, String tableId) throws TableNotFoundException, AccumuloException {
    String tableName = Tables.getTableName(conn.getInstance(), tableId);
    return new ConfigurationCopy(conn.tableOperations().getProperties(tableName));
  }

  /**
   * Gets the maximum number of files per tablet from this configuration.
   *
   * @return maximum number of files per tablet
   * @see Property#TABLE_FILE_MAX
   * @see Property#TSERV_SCAN_MAX_OPENFILES
   */
  public int getMaxFilesPerTablet() {
    int maxFilesPerTablet = getCount(Property.TABLE_FILE_MAX);
    if (maxFilesPerTablet <= 0) {
      maxFilesPerTablet = getCount(Property.TSERV_SCAN_MAX_OPENFILES) - 1;
      log.debug("Max files per tablet " + maxFilesPerTablet);
    }

    return maxFilesPerTablet;
  }

  /**
   * Invalidates the <code>ZooCache</code> used for storage and quick retrieval of properties for this configuration.
   */
  public void invalidateCache() {}

  /**
   * Creates a new instance of a class specified in a configuration property.
   *
   * @param property
   *          property specifying class name
   * @param base
   *          base class of type
   * @param defaultInstance
   *          instance to use if creation fails
   * @return new class instance, or default instance if creation failed
   * @see AccumuloVFSClassLoader
   */
  public <T> T instantiateClassProperty(Property property, Class<T> base, T defaultInstance) {
    String clazzName = get(property);
    T instance = null;

    try {
      Class<? extends T> clazz = AccumuloVFSClassLoader.loadClass(clazzName, base);
      instance = clazz.newInstance();
      log.info("Loaded class : " + clazzName);
    } catch (Exception e) {
      log.warn("Failed to load class ", e);
    }

    if (instance == null) {
      log.info("Using " + defaultInstance.getClass().getName());
      instance = defaultInstance;
    }
    return instance;
  }

}
