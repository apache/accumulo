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
package org.apache.accumulo.core.conf;

import static org.apache.accumulo.core.conf.Property.GENERAL_ARBITRARY_PROP_PREFIX;
import static org.apache.accumulo.core.conf.Property.INSTANCE_CRYPTO_PREFIX;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.conf.PropertyType.PortRange;
import org.apache.accumulo.core.spi.scan.SimpleScanDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

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
  private final Lock prefixCacheUpdateLock = new ReentrantLock();

  private static final Logger log = LoggerFactory.getLogger(AccumuloConfiguration.class);

  /**
   * Gets a property value from this configuration.
   *
   * <p>
   * Note: this is inefficient for values that are not a {@link Property}. For retrieving multiple
   * properties, use {@link #getProperties(Map, Predicate)} with a custom filter.
   *
   * @param property property to get
   * @return property value
   */
  public String get(String property) {
    Property p = Property.getPropertyByKey(property);
    if (p != null) {
      return get(p);
    } else {
      Map<String,String> propMap = new HashMap<>(1);
      getProperties(propMap, key -> Objects.equals(property, key));
      return propMap.get(property);
    }
  }

  /**
   * Gets a property value from this configuration.
   *
   * @param property property to get
   * @return property value
   */
  public abstract String get(Property property);

  /**
   * Given a property that is not deprecated and an ordered list of deprecated properties, determine
   * which one to use based on which is set by the user in the configuration. If the non-deprecated
   * property is set, use that. Otherwise, use the first deprecated property that is set. If no
   * deprecated properties are set, use the non-deprecated property by default, even though it is
   * not set (since it is not set, it will resolve to its default value). Since the deprecated
   * properties are checked in order, newer properties should be on the left, replacing older
   * properties on the right, so if a newer property is set, it will be selected over any older
   * property that may also be set.
   */
  public Property resolve(Property property, Property... deprecated) {
    if (property.isDeprecated()) {
      throw new IllegalArgumentException("Unexpected deprecated " + property.name());
    }
    for (Property p : deprecated) {
      if (!p.isDeprecated()) {
        var notDeprecated = Stream.of(deprecated).filter(Predicate.not(Property::isDeprecated))
            .map(Property::name).collect(Collectors.toList());
        throw new IllegalArgumentException("Unexpected non-deprecated " + notDeprecated);
      }
    }
    return isPropertySet(property) ? property
        : Stream.of(deprecated).filter(this::isPropertySet).findFirst().orElse(property);
  }

  /**
   * Returns property key/value pairs in this configuration. The pairs include those defined in this
   * configuration which pass the given filter, and those supplied from the parent configuration
   * which are not included from here.
   *
   * @param props properties object to populate
   * @param filter filter for accepting properties from this configuration
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
   * @param property prefix property, must be of type PropertyType.PREFIX
   * @return a map of property keys to values
   * @throws IllegalArgumentException if property is not a prefix
   */
  public Map<String,String> getAllPropertiesWithPrefix(Property property) {
    checkType(property, PropertyType.PREFIX);

    PrefixProps prefixProps = cachedPrefixProps.get(property);

    long currentCount = getUpdateCount();

    if (prefixProps == null || prefixProps.updateCount != currentCount) {
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

  public Map<String,String> getAllPropertiesWithPrefixStripped(Property prefix) {
    final var builder = ImmutableMap.<String,String>builder();
    getAllPropertiesWithPrefix(prefix).forEach((k, v) -> {
      String optKey = k.substring(prefix.getKey().length());
      builder.put(optKey, v);
    });

    return builder.build();
  }

  public Map<String,String> getAllCryptoProperties() {
    Map<String,String> allProps = new HashMap<>();
    allProps.putAll(getAllPropertiesWithPrefix(INSTANCE_CRYPTO_PREFIX));
    allProps.putAll(getAllPropertiesWithPrefix(GENERAL_ARBITRARY_PROP_PREFIX));
    allProps.putAll(getAllPropertiesWithPrefix(TABLE_CRYPTO_PREFIX));
    return allProps;
  }

  /**
   * Gets a property of type {@link PropertyType#BYTES} or {@link PropertyType#MEMORY}, interpreting
   * the value properly.
   *
   * @param property Property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
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
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
   */
  public long getTimeInMillis(Property property) {
    checkType(property, PropertyType.TIMEDURATION);

    return ConfigurationTypeHelper.getTimeInMillis(get(property));
  }

  /**
   * Gets a property of type {@link PropertyType#BOOLEAN}, interpreting the value properly (using
   * <code>Boolean.parseBoolean()</code>).
   *
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
   */
  public boolean getBoolean(Property property) {
    checkType(property, PropertyType.BOOLEAN);
    return Boolean.parseBoolean(get(property));
  }

  /**
   * Gets a property of type {@link PropertyType#FRACTION}, interpreting the value properly.
   *
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
   */
  public double getFraction(Property property) {
    checkType(property, PropertyType.FRACTION);

    return ConfigurationTypeHelper.getFraction(get(property));
  }

  /**
   * Gets a property of type {@link PropertyType#PORT}, interpreting the value properly (as an
   * integer within the range of non-privileged ports). Consider using
   * {@link #getPortStream(Property)}, if an array is not needed.
   *
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
   */
  public int[] getPort(Property property) {
    return getPortStream(property).toArray();
  }

  /**
   * Same as {@link #getPort(Property)}, but as an {@link IntStream}.
   */
  public IntStream getPortStream(Property property) {
    checkType(property, PropertyType.PORT);

    String portString = get(property);
    try {
      return PortRange.parse(portString);
    } catch (IllegalArgumentException e) {
      try {
        int port = Integer.parseInt(portString);
        if (port == 0 || PortRange.VALID_RANGE.contains(port)) {
          return IntStream.of(port);
        } else {
          log.error("Invalid port number {}; Using default {}", port, property.getDefaultValue());
          return IntStream.of(Integer.parseInt(property.getDefaultValue()));
        }
      } catch (NumberFormatException e1) {
        throw new IllegalArgumentException("Invalid port syntax. Must be a single positive "
            + "integers or a range (M-N) of positive integers");
      }
    }
  }

  /**
   * Gets a property of type {@link PropertyType#COUNT}, interpreting the value properly (as an
   * integer).
   *
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
   */
  public int getCount(Property property) {
    checkType(property, PropertyType.COUNT);

    String countString = get(property);
    return Integer.parseInt(countString);
  }

  /**
   * Gets a property of type {@link PropertyType#PATH}.
   *
   * @param property property to get
   * @return property value
   * @throws IllegalArgumentException if the property is of the wrong type
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
    public final boolean isScanServer;

    public ScanExecutorConfig(String name, int maxThreads, OptionalInt priority,
        Optional<String> comparatorFactory, Map<String,String> comparatorFactoryOpts,
        boolean isScanServer) {
      this.name = name;
      this.maxThreads = maxThreads;
      this.priority = priority;
      this.prioritizerClass = comparatorFactory;
      this.prioritizerOpts = comparatorFactoryOpts;
      this.isScanServer = isScanServer;
    }

    /**
     * Re-reads the max threads from the configuration that created this class
     */
    public int getCurrentMaxThreads() {
      Integer depThreads = getDeprecatedScanThreads(name, isScanServer);
      if (depThreads != null) {
        return depThreads;
      }

      if (isScanServer) {
        String prop =
            Property.SSERV_SCAN_EXECUTORS_PREFIX.getKey() + name + "." + SCAN_EXEC_THREADS;
        String val = getAllPropertiesWithPrefix(Property.SSERV_SCAN_EXECUTORS_PREFIX).get(prop);
        return Integer.parseInt(val);
      } else {
        String prop =
            Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + name + "." + SCAN_EXEC_THREADS;
        String val = getAllPropertiesWithPrefix(Property.TSERV_SCAN_EXECUTORS_PREFIX).get(prop);
        return Integer.parseInt(val);
      }
    }
  }

  public abstract boolean isPropertySet(Property prop);

  // deprecation property warning could get spammy in tserver so only warn once
  boolean depPropWarned = false;

  @SuppressWarnings("deprecation")
  Integer getDeprecatedScanThreads(String name, boolean isScanServer) {

    Property prop;
    Property deprecatedProp;

    if (name.equals(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME)) {
      prop = isScanServer ? Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS
          : Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS;
      deprecatedProp = Property.TSERV_READ_AHEAD_MAXCONCURRENT;
    } else if (name.equals("meta")) {
      prop = isScanServer ? Property.SSERV_SCAN_EXECUTORS_META_THREADS
          : Property.TSERV_SCAN_EXECUTORS_META_THREADS;
      deprecatedProp = Property.TSERV_METADATA_READ_AHEAD_MAXCONCURRENT;
    } else {
      return null;
    }

    if (!isPropertySet(prop) && isPropertySet(deprecatedProp)) {
      if (!depPropWarned) {
        depPropWarned = true;
        log.warn("Property {} is deprecated, use {} instead.", deprecatedProp.getKey(),
            prop.getKey());
      }
      return Integer.valueOf(get(deprecatedProp));
    } else if (isPropertySet(prop) && isPropertySet(deprecatedProp) && !depPropWarned) {
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
     * working on unrelated tasks would not impede each other because of accessing config.
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
   * @param converter This functions is used to create an object from configuration. A reference to
   *        this function will be kept and called by the returned deriver.
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

  public Collection<ScanExecutorConfig> getScanExecutors(boolean isScanServer) {

    Property prefix =
        isScanServer ? Property.SSERV_SCAN_EXECUTORS_PREFIX : Property.TSERV_SCAN_EXECUTORS_PREFIX;

    Map<String,Map<String,String>> propsByName = new HashMap<>();

    List<ScanExecutorConfig> scanResources = new ArrayList<>();

    for (Entry<String,String> entry : getAllPropertiesWithPrefix(prefix).entrySet()) {

      String suffix = entry.getKey().substring(prefix.getKey().length());
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
          Integer depThreads = getDeprecatedScanThreads(name, isScanServer);
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
          throw new IllegalStateException("Unknown scan executor option : " + opt);
        }
      }

      Preconditions.checkArgument(threads != null && threads > 0,
          "Scan resource %s incorrectly specified threads", name);

      scanResources.add(new ScanExecutorConfig(name, threads,
          prio == null ? OptionalInt.empty() : OptionalInt.of(prio),
          Optional.ofNullable(prioritizerClass), prioritizerOpts, isScanServer));
    }

    return scanResources;
  }

  /**
   * Invalidates the <code>ZooCache</code> used for storage and quick retrieval of properties for
   * this configuration.
   */
  public void invalidateCache() {}

  /**
   * get a parent configuration or null if it does not exist.
   *
   * @since 2.1.0
   */
  public AccumuloConfiguration getParent() {
    return null;
  }

  public Stream<Entry<String,String>> stream() {
    return StreamSupport.stream(this.spliterator(), false);
  }
}
