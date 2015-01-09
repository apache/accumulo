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
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.log4j.Logger;

public abstract class AccumuloConfiguration implements Iterable<Entry<String,String>> {

  public interface PropertyFilter {
    boolean accept(String key);
  }

  public static class AllFilter implements PropertyFilter {
    @Override
    public boolean accept(String key) {
      return true;
    }
  }

  public static class PrefixFilter implements PropertyFilter {

    private String prefix;

    public PrefixFilter(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public boolean accept(String key) {
      return key.startsWith(prefix);
    }
  }

  private static final Logger log = Logger.getLogger(AccumuloConfiguration.class);

  public abstract String get(Property property);

  public abstract void getProperties(Map<String,String> props, PropertyFilter filter);

  @Override
  public Iterator<Entry<String,String>> iterator() {
    TreeMap<String,String> entries = new TreeMap<String,String>();
    getProperties(entries, new AllFilter());
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
   * This method returns all properties in a map of string->string under the given prefix property.
   *
   * @param property
   *          the prefix property, and must be of type PropertyType.PREFIX
   * @return a map of strings to strings of the resulting properties
   */
  public Map<String,String> getAllPropertiesWithPrefix(Property property) {
    checkType(property, PropertyType.PREFIX);

    Map<String,String> propMap = new HashMap<String,String>();
    getProperties(propMap, new PrefixFilter(property.getKey()));
    return propMap;
  }

  public long getMemoryInBytes(Property property) {
    checkType(property, PropertyType.MEMORY);

    String memString = get(property);
    return getMemoryInBytes(memString);
  }

  static public long getMemoryInBytes(String str) {
    int multiplier = 0;
    char lastChar = str.charAt(str.length() - 1);

    if (lastChar == 'b') {
      log.warn("The 'b' in " + str + " is being considered as bytes. " + "Setting memory by bits is not supported");
    }
    try {
      switch (Character.toUpperCase(lastChar)) {
        case 'G':
          multiplier += 10;
        case 'M':
          multiplier += 10;
        case 'K':
          multiplier += 10;
        case 'B':
          return Long.parseLong(str.substring(0, str.length() - 1)) << multiplier;
        default:
          return Long.parseLong(str);
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException("The value '" + str + "' is not a valid memory setting. A valid value would a number "
          + "possibily followed by an optional 'G', 'M', 'K', or 'B'.");
    }
  }

  public long getTimeInMillis(Property property) {
    checkType(property, PropertyType.TIMEDURATION);

    return getTimeInMillis(get(property));
  }

  static public long getTimeInMillis(String str) {
    int multiplier = 1;
    switch (str.charAt(str.length() - 1)) {
      case 'd':
        multiplier *= 24;
      case 'h':
        multiplier *= 60;
      case 'm':
        multiplier *= 60;
      case 's':
        multiplier *= 1000;
        if (str.length() > 1 && str.endsWith("ms")) // millis
          // case
          return Long.parseLong(str.substring(0, str.length() - 2));
        return Long.parseLong(str.substring(0, str.length() - 1)) * multiplier;
      default:
        return Long.parseLong(str) * 1000;
    }
  }

  public boolean getBoolean(Property property) {
    checkType(property, PropertyType.BOOLEAN);
    return Boolean.parseBoolean(get(property));
  }

  public double getFraction(Property property) {
    checkType(property, PropertyType.FRACTION);

    return getFraction(get(property));
  }

  public double getFraction(String str) {
    if (str.charAt(str.length() - 1) == '%')
      return Double.parseDouble(str.substring(0, str.length() - 1)) / 100.0;
    return Double.parseDouble(str);
  }

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

  public int getCount(Property property) {
    checkType(property, PropertyType.COUNT);

    String countString = get(property);
    return Integer.parseInt(countString);
  }

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

  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    return DefaultConfiguration.getInstance();
  }

  public static AccumuloConfiguration getTableConfiguration(Connector conn, String tableId) throws TableNotFoundException, AccumuloException {
    String tableName = Tables.getTableName(conn.getInstance(), tableId);
    return new ConfigurationCopy(conn.tableOperations().getProperties(tableName));
  }

  public int getMaxFilesPerTablet() {
    int maxFilesPerTablet = getCount(Property.TABLE_FILE_MAX);
    if (maxFilesPerTablet <= 0) {
      maxFilesPerTablet = getCount(Property.TSERV_SCAN_MAX_OPENFILES) - 1;
      log.debug("Max files per tablet " + maxFilesPerTablet);
    }

    return maxFilesPerTablet;
  }

  // overridden in ZooConfiguration
  public void invalidateCache() {}

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
