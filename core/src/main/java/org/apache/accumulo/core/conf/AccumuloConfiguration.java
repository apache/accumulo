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
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.PropertyType.PortRange;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configuration object.
 */
public abstract class AccumuloConfiguration implements Iterable<Entry<String,String>> {

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
    Map<String,String> propMap = new HashMap<>(1);
    getProperties(propMap, key -> Objects.equals(property, key));
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
    Predicate<String> all = x -> true;
    TreeMap<String,String> entries = new TreeMap<>();
    getProperties(entries, all);
    return entries.entrySet().iterator();
  }

  private static void checkType(Property property, PropertyType type) {
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

    Map<String,String> propMap = new HashMap<>();
    getProperties(propMap, key -> key.startsWith(property.getKey()));
    return propMap;
  }

  /**
   * Gets a property of type {@link PropertyType#BYTES} or {@link PropertyType#MEMORY}, interpreting the value properly.
   *
   * @param property
   *          Property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   */
  public long getAsBytes(Property property) {
    String memString = get(property);
    if (property.getType() == PropertyType.MEMORY) {
      return ConfigurationTypeHelper.getMemoryAsBytes(memString);
    } else if (property.getType() == PropertyType.BYTES) {
      return ConfigurationTypeHelper.getFixedMemoryAsBytes(memString);
    } else {
      throw new IllegalArgumentException(property.getKey() + " is not of BYTES or MEMORY type");
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
   */
  public long getTimeInMillis(Property property) {
    checkType(property, PropertyType.TIMEDURATION);

    return ConfigurationTypeHelper.getTimeInMillis(get(property));
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
   */
  public double getFraction(Property property) {
    checkType(property, PropertyType.FRACTION);

    return ConfigurationTypeHelper.getFraction(get(property));
  }

  /**
   * Gets a property of type {@link PropertyType#PORT}, interpreting the value properly (as an integer within the range of non-privileged ports).
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   */
  public int[] getPort(Property property) {
    checkType(property, PropertyType.PORT);

    String portString = get(property);
    int[] ports = null;
    try {
      Pair<Integer,Integer> portRange = PortRange.parse(portString);
      int low = portRange.getFirst();
      int high = portRange.getSecond();
      ports = new int[high - low + 1];
      for (int i = 0, j = low; j <= high; i++, j++) {
        ports[i] = j;
      }
    } catch (IllegalArgumentException e) {
      ports = new int[1];
      try {
        int port = Integer.parseInt(portString);
        if (port != 0) {
          if (port < 1024 || port > 65535) {
            log.error("Invalid port number {}; Using default {}", port, property.getDefaultValue());
            ports[0] = Integer.parseInt(property.getDefaultValue());
          } else {
            ports[0] = port;
          }
        } else {
          ports[0] = port;
        }
      } catch (NumberFormatException e1) {
        throw new IllegalArgumentException("Invalid port syntax. Must be a single positive integers or a range (M-N) of positive integers");
      }
    }
    return ports;
  }

  /**
   * Gets a property of type {@link PropertyType#COUNT}, interpreting the value properly (as an integer).
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
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
      log.debug("Max files per tablet {}", maxFilesPerTablet);
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
      log.info("Loaded class : {}", clazzName);
    } catch (Exception e) {
      log.warn("Failed to load class ", e);
    }

    if (instance == null) {
      log.info("Using {}", defaultInstance.getClass().getName());
      instance = defaultInstance;
    }
    return instance;
  }

}
