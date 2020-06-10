/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.PropertyType.PortRange;
import org.apache.accumulo.core.spi.scan.SimpleScanDispatcher;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A configuration object.
 */
public abstract class AccumuloConfiguration implements Iterable<Entry<String,String>> {

  private static class PrefixProps {
    final long updateCount;
    final Map<String,String> props;

    PrefixProps(Map<String,String> props, long updateCount) {
      this.updateCount = updateCount;
      this.props = props;
    }
  }

  private volatile EnumMap<Property,PrefixProps> cachedPrefixProps = new EnumMap<>(Property.class);
  private Lock prefixCacheUpdateLock = new ReentrantLock();

  private static final Logger log = LoggerFactory.getLogger(AccumuloConfiguration.class);

  /**
   * Gets a property value from this configuration.
   *
   * <p>
   * Note: this is inefficient, but convenient on occasion. For retrieving multiple properties, use
   * {@link #getProperties(Map, Predicate)} with a custom filter.
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
   * Given a property and a deprecated property determine which one to use base on which one is set.
   */
  public Property resolve(Property property, Property deprecatedProperty) {
    if (isPropertySet(property, true)) {
      return property;
    } else if (isPropertySet(deprecatedProperty, true)) {
      return deprecatedProperty;
    } else {
      return property;
    }
  }

  /**
   * Returns property key/value pairs in this configuration. The pairs include those defined in this
   * configuration which pass the given filter, and those supplied from the parent configuration
   * which are not included from here.
   *
   * @param props
   *          properties object to populate
   * @param filter
   *          filter for accepting properties from this configuration
   */
  public abstract void getProperties(Map<String,String> props, Predicate<String> filter);

  /**
   * Returns an iterator over property key/value pairs in this configuration. Some implementations
   * may elect to omit properties.
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
      String msg = "Configuration method intended for type " + type + " called with a "
          + property.getType() + " argument (" + property.getKey() + ")";
      IllegalArgumentException err = new IllegalArgumentException(msg);
      log.error(msg, err);
      throw err;
    }
  }

  /**
   * Each time configuration changes, this counter should increase. Anything that caches information
   * that is derived from configuration can use this method to know when to update.
   */
  public long getUpdateCount() {
    return 0;
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

    PrefixProps prefixProps = cachedPrefixProps.get(property);

    if (prefixProps == null || prefixProps.updateCount != getUpdateCount()) {
      prefixCacheUpdateLock.lock();
      try {
        // Very important that update count is read before getting properties. Also only read it
        // once.
        long updateCount = getUpdateCount();
        prefixProps = cachedPrefixProps.get(property);

        if (prefixProps == null || prefixProps.updateCount != updateCount) {
          Map<String,String> propMap = new HashMap<>();
          // The reason this caching exists is to avoid repeatedly making this expensive call.
          getProperties(propMap, key -> key.startsWith(property.getKey()));
          propMap = Map.copyOf(propMap);

          // So that locking is not needed when reading from enum map, always create a new one.
          // Construct and populate map using a local var so its not visible
          // until ready.
          EnumMap<Property,PrefixProps> localPrefixes = new EnumMap<>(Property.class);

          // carry forward any other cached prefixes
          localPrefixes.putAll(cachedPrefixProps);

          // put the updates
          prefixProps = new PrefixProps(propMap, updateCount);
          localPrefixes.put(property, prefixProps);

          // make the newly constructed map available
          cachedPrefixProps = localPrefixes;
        }
      } finally {
        prefixCacheUpdateLock.unlock();
      }
    }

    return prefixProps.props;
  }

  /**
   * Gets a property of type {@link PropertyType#BYTES} or {@link PropertyType#MEMORY}, interpreting
   * the value properly.
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
   * Gets a property of type {@link PropertyType#BOOLEAN}, interpreting the value properly (using
   * <code>Boolean.parseBoolean()</code>).
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
   * Gets a property of type {@link PropertyType#PORT}, interpreting the value properly (as an
   * integer within the range of non-privileged ports).
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
        if (port == 0) {
          ports[0] = port;
        } else {
          if (port < 1024 || port > 65535) {
            log.error("Invalid port number {}; Using default {}", port, property.getDefaultValue());
            ports[0] = Integer.parseInt(property.getDefaultValue());
          } else {
            ports[0] = port;
          }
        }
      } catch (NumberFormatException e1) {
        throw new IllegalArgumentException("Invalid port syntax. Must be a single positive "
            + "integers or a range (M-N) of positive integers");
      }
    }
    return ports;
  }

  /**
   * Gets a property of type {@link PropertyType#COUNT}, interpreting the value properly (as an
   * integer).
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
   * Gets a property of type {@link PropertyType#PATH}.
   *
   * @param property
   *          property to get
   * @return property value
   * @throws IllegalArgumentException
   *           if the property is of the wrong type
   */
  public String getPath(Property property) {
    checkType(property, PropertyType.PATH);

    String pathString = get(property);
    if (pathString == null) {
      return null;
    }

    if (pathString.contains("$ACCUMULO_")) {
      throw new IllegalArgumentException("Environment variable interpolation not supported here. "
          + "Consider using '${env:ACCUMULO_HOME}' or similar in your configuration file.");
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

  public class ScanExecutorConfig {
    public final String name;
    public final int maxThreads;
    public final OptionalInt priority;
    public final Optional<String> prioritizerClass;
    public final Map<String,String> prioritizerOpts;

    public ScanExecutorConfig(String name, int maxThreads, OptionalInt priority,
        Optional<String> comparatorFactory, Map<String,String> comparatorFactoryOpts) {
      this.name = name;
      this.maxThreads = maxThreads;
      this.priority = priority;
      this.prioritizerClass = comparatorFactory;
      this.prioritizerOpts = comparatorFactoryOpts;
    }

    /**
     * Re-reads the max threads from the configuration that created this class
     */
    public int getCurrentMaxThreads() {
      Integer depThreads = getDeprecatedScanThreads(name);
      if (depThreads != null) {
        return depThreads;
      }

      String prop = Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + name + "." + SCAN_EXEC_THREADS;
      String val = getAllPropertiesWithPrefix(Property.TSERV_SCAN_EXECUTORS_PREFIX).get(prop);
      return Integer.parseInt(val);
    }
  }

  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    throw new UnsupportedOperationException();
  }

  // deprecation property warning could get spammy in tserver so only warn once
  boolean depPropWarned = false;

  @SuppressWarnings("deprecation")
  Integer getDeprecatedScanThreads(String name) {

    Property prop;
    Property deprecatedProp;

    if (name.equals(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME)) {
      prop = Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS;
      deprecatedProp = Property.TSERV_READ_AHEAD_MAXCONCURRENT;
    } else if (name.equals("meta")) {
      prop = Property.TSERV_SCAN_EXECUTORS_META_THREADS;
      deprecatedProp = Property.TSERV_METADATA_READ_AHEAD_MAXCONCURRENT;
    } else {
      return null;
    }

    if (!isPropertySet(prop, true) && isPropertySet(deprecatedProp, true)) {
      if (!depPropWarned) {
        depPropWarned = true;
        log.warn("Property {} is deprecated, use {} instead.", deprecatedProp.getKey(),
            prop.getKey());
      }
      return Integer.valueOf(get(deprecatedProp));
    } else if (isPropertySet(prop, true) && isPropertySet(deprecatedProp, true) && !depPropWarned) {
      depPropWarned = true;
      log.warn("Deprecated property {} ignored because {} is set", deprecatedProp.getKey(),
          prop.getKey());
    }

    return null;
  }

  private static class RefCount<T> {
    T obj;
    long count;

    RefCount(long c, T r) {
      this.count = c;
      this.obj = r;
    }
  }

  private class DeriverImpl<T> implements Deriver<T> {

    private final AtomicReference<RefCount<T>> refref = new AtomicReference<>();
    private final Function<AccumuloConfiguration,T> converter;

    DeriverImpl(Function<AccumuloConfiguration,T> converter) {
      this.converter = converter;
    }

    /**
     * This method was written with the goal of avoiding thread contention and minimizing
     * recomputation. Configuration can be accessed frequently by many threads. Ideally, threads
     * working on unrelated task would not impeded each other because of accessing config.
     *
     * To avoid thread contention, synchronization and needless calls to compare and set were
     * avoided. For example if 100 threads are all calling compare and set in a loop this could
     * cause significant contention.
     */
    @Override
    public T derive() {

      // very important to obtain this before possibly recomputing object
      long uc = getUpdateCount();

      RefCount<T> rc = refref.get();

      if (rc == null || rc.count != uc) {
        T newObj = converter.apply(AccumuloConfiguration.this);

        // very important to record the update count that was obtained before recomputing.
        RefCount<T> nrc = new RefCount<>(uc, newObj);

        /*
         * The return value of compare and set is intentionally ignored here. This code could loop
         * calling compare and set inorder to avoid returning a stale object. However after this
         * function returns, the object could immediately become stale. So in the big picture stale
         * objects can not be prevented. Looping here could cause thread contention, but it would
         * not solve the overall stale object problem. That is why the return value was ignored. The
         * following line is a least effort attempt to make the result of this recomputation
         * available to the next caller.
         */
        refref.compareAndSet(rc, nrc);

        return nrc.obj;
      }

      return rc.obj;
    }
  }

  /**
   * Automatically regenerates an object whenever configuration changes. When configuration is not
   * changing, keeps returning the same object. Implementations should be thread safe and eventually
   * consistent. See {@link AccumuloConfiguration#newDeriver(Function)}
   */
  public interface Deriver<T> {
    T derive();
  }

  /**
   * Enables deriving an object from configuration and automatically deriving a new object any time
   * configuration changes.
   *
   * @param converter
   *          This functions is used to create an object from configuration. A reference to this
   *          function will be kept and called by the returned deriver.
   * @return The returned supplier will automatically re-derive the object any time this
   *         configuration changes. When configuration is not changing, the same object is returned.
   *
   */
  public <T> Deriver<T> newDeriver(Function<AccumuloConfiguration,T> converter) {
    return new DeriverImpl<>(converter);
  }

  private static final String SCAN_EXEC_THREADS = "threads";
  private static final String SCAN_EXEC_PRIORITY = "priority";
  private static final String SCAN_EXEC_PRIORITIZER = "prioritizer";
  private static final String SCAN_EXEC_PRIORITIZER_OPTS = "prioritizer.opts.";

  public Collection<ScanExecutorConfig> getScanExecutors() {

    Map<String,Map<String,String>> propsByName = new HashMap<>();

    List<ScanExecutorConfig> scanResources = new ArrayList<>();

    for (Entry<String,String> entry : getAllPropertiesWithPrefix(
        Property.TSERV_SCAN_EXECUTORS_PREFIX).entrySet()) {

      String suffix =
          entry.getKey().substring(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey().length());
      String[] tokens = suffix.split("\\.", 2);
      String name = tokens[0];

      propsByName.computeIfAbsent(name, k -> new HashMap<>()).put(tokens[1], entry.getValue());
    }

    for (Entry<String,Map<String,String>> entry : propsByName.entrySet()) {
      String name = entry.getKey();
      Integer threads = null;
      Integer prio = null;
      String prioritizerClass = null;
      Map<String,String> prioritizerOpts = new HashMap<>();

      for (Entry<String,String> subEntry : entry.getValue().entrySet()) {
        String opt = subEntry.getKey();
        String val = subEntry.getValue();

        if (opt.equals(SCAN_EXEC_THREADS)) {
          Integer depThreads = getDeprecatedScanThreads(name);
          if (depThreads == null) {
            threads = Integer.parseInt(val);
          } else {
            threads = depThreads;
          }
        } else if (opt.equals(SCAN_EXEC_PRIORITY)) {
          prio = Integer.parseInt(val);
        } else if (opt.equals(SCAN_EXEC_PRIORITIZER)) {
          prioritizerClass = val;
        } else if (opt.startsWith(SCAN_EXEC_PRIORITIZER_OPTS)) {
          String key = opt.substring(SCAN_EXEC_PRIORITIZER_OPTS.length());
          if (key.isEmpty()) {
            throw new IllegalStateException("Invalid scan executor option : " + opt);
          }
          prioritizerOpts.put(key, val);
        } else {
          throw new IllegalStateException("Unkown scan executor option : " + opt);
        }
      }

      Preconditions.checkArgument(threads != null && threads > 0,
          "Scan resource %s incorrectly specified threads", name);

      scanResources.add(new ScanExecutorConfig(name, threads,
          prio == null ? OptionalInt.empty() : OptionalInt.of(prio),
          Optional.ofNullable(prioritizerClass), prioritizerOpts));
    }

    return scanResources;
  }

  /**
   * Invalidates the <code>ZooCache</code> used for storage and quick retrieval of properties for
   * this configuration.
   */
  public void invalidateCache() {}
}
