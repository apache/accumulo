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
package org.apache.accumulo.core.iteratorsImpl;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.NamespaceOperationsHelper;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for configuring iterators. These methods were moved from IteratorUtil so that it
 * could be treated as API.
 */
public class IteratorConfigUtil {
  private static final Logger log = LoggerFactory.getLogger(IteratorConfigUtil.class);

  public static final Comparator<IterInfo> ITER_INFO_COMPARATOR =
      Comparator.comparingInt(IterInfo::getPriority);

  private static final String WARNING_MSG =
      ". Iterator was set as requested, but may lead to non-deterministic behavior.";

  /**
   * Fetch the correct configuration key prefix for the given scope. Throws an
   * IllegalArgumentException if no property exists for the given scope.
   */
  public static Property getProperty(IteratorScope scope) {
    requireNonNull(scope);
    switch (scope) {
      case scan:
        return Property.TABLE_ITERATOR_SCAN_PREFIX;
      case minc:
        return Property.TABLE_ITERATOR_MINC_PREFIX;
      case majc:
        return Property.TABLE_ITERATOR_MAJC_PREFIX;
      default:
        throw new IllegalStateException("Could not find configuration property for IteratorScope");
    }
  }

  /**
   * Get the initial (default) properties for a table. This includes
   * {@link #getInitialTableIterators()} and a constraint {@link DefaultKeySizeConstraint}
   *
   * @return A map of default Table properties
   */
  public static Map<String,String> getInitialTableProperties() {
    TreeMap<String,String> props = new TreeMap<>(getInitialTableIterators());

    props.put(Property.TABLE_CONSTRAINT_PREFIX + "1", DefaultKeySizeConstraint.class.getName());

    return props;
  }

  /**
   * For all iterator scopes, includes a {@link VersioningIterator} at priority 20 that retains a
   * single version of a given K/V pair.
   *
   * @return a map of default Table iterator properties
   * @see #getInitialTableIteratorSettings
   */
  public static Map<String,String> getInitialTableIterators() {
    TreeMap<String,String> props = new TreeMap<>();

    for (IteratorScope iterScope : IteratorScope.values()) {
      props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers",
          "20," + VersioningIterator.class.getName());
      props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions", "1");
    }

    return props;
  }

  /**
   *
   * @return a map of the default Table iterator settings
   * @see #getInitialTableIterators
   */
  public static Map<IteratorSetting,EnumSet<IteratorScope>> getInitialTableIteratorSettings() {
    return Map.of(new IteratorSetting(20, "vers", VersioningIterator.class.getName(),
        Map.of("maxVersions", "1")), EnumSet.allOf(IteratorScope.class));
  }

  public static List<IterInfo> parseIterConf(IteratorScope scope, List<IterInfo> iters,
      Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
    Map<String,String> properties = conf.getAllPropertiesWithPrefix(getProperty(scope));
    ArrayList<IterInfo> iterators = new ArrayList<>(iters);

    for (Entry<String,String> entry : properties.entrySet()) {
      var iterProp = IteratorProperty.parse(entry.getKey(), entry.getValue());
      if (iterProp.isOption()) {
        allOptions.computeIfAbsent(iterProp.getName(), k -> new HashMap<>())
            .put(iterProp.getOptionKey(), iterProp.getOptionValue());
      } else {
        iterators
            .add(new IterInfo(iterProp.getPriority(), iterProp.getClassName(), iterProp.getName()));
      }
    }

    iterators.sort(ITER_INFO_COMPARATOR);
    return iterators;
  }

  public static void mergeIteratorConfig(List<IterInfo> destList,
      Map<String,Map<String,String>> destOpts, List<IterInfo> tableIters,
      Map<String,Map<String,String>> tableOpts, List<IterInfo> ssi,
      Map<String,Map<String,String>> ssio) {
    destList.addAll(tableIters);
    destList.addAll(ssi);
    destList.sort(ITER_INFO_COMPARATOR);

    Set<Entry<String,Map<String,String>>> es = tableOpts.entrySet();
    for (Entry<String,Map<String,String>> entry : es) {
      if (entry.getValue() == null) {
        destOpts.put(entry.getKey(), null);
      } else {
        destOpts.put(entry.getKey(), new HashMap<>(entry.getValue()));
      }
    }

    mergeOptions(ssio, destOpts);

  }

  private static void mergeOptions(Map<String,Map<String,String>> ssio,
      Map<String,Map<String,String>> allOptions) {
    ssio.forEach((k, v) -> {
      if (v != null) {
        Map<String,String> options = allOptions.get(k);
        if (options == null) {
          allOptions.put(k, v);
        } else {
          options.putAll(v);
        }
      }
    });
  }

  public static IteratorBuilder.IteratorBuilderEnv loadIterConf(IteratorScope scope,
      List<IterInfo> iters, Map<String,Map<String,String>> iterOpts, AccumuloConfiguration conf) {
    Map<String,Map<String,String>> allOptions = new HashMap<>();
    List<IterInfo> iterators = parseIterConf(scope, iters, allOptions, conf);
    mergeOptions(iterOpts, allOptions);
    return IteratorBuilder.builder(iterators).opts(allOptions);
  }

  /**
   * Convert the list of iterators to IterInfo objects and then load the stack.
   */
  public static SortedKeyValueIterator<Key,Value> convertItersAndLoad(IteratorScope scope,
      SortedKeyValueIterator<Key,Value> source, AccumuloConfiguration conf,
      List<IteratorSetting> iterators, IteratorEnvironment env)
      throws IOException, ReflectiveOperationException {

    List<IterInfo> ssiList = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();

    for (IteratorSetting is : iterators) {
      ssiList.add(new IterInfo(is.getPriority(), is.getIteratorClass(), is.getName()));
      ssio.put(is.getName(), is.getOptions());
    }

    var ibEnv = loadIterConf(scope, ssiList, ssio, conf);
    var iterBuilder = ibEnv.env(env).useClassLoader(ClassLoaderUtil.tableContext(conf)).build();
    return loadIterators(source, iterBuilder);
  }

  /**
   * Load a stack of iterators provided in the iterator builder, starting with source.
   */
  public static SortedKeyValueIterator<Key,Value>
      loadIterators(SortedKeyValueIterator<Key,Value> source, IteratorBuilder iteratorBuilder)
          throws IOException, ReflectiveOperationException {
    SortedKeyValueIterator<Key,Value> prev = source;
    final boolean useClassLoader = iteratorBuilder.useAccumuloClassLoader;
    Map<String,Class<SortedKeyValueIterator<Key,Value>>> classCache = new HashMap<>();

    for (IterInfo iterInfo : iteratorBuilder.iters) {
      try {
        Class<SortedKeyValueIterator<Key,Value>> clazz = null;
        log.trace("Attempting to load iterator class {}", iterInfo.className);
        if (iteratorBuilder.useClassCache) {
          clazz = classCache.get(iterInfo.className);

          if (clazz == null) {
            clazz = loadClass(useClassLoader, iteratorBuilder.context, iterInfo);
            classCache.put(iterInfo.className, clazz);
          }
        } else {
          clazz = loadClass(useClassLoader, iteratorBuilder.context, iterInfo);
        }

        SortedKeyValueIterator<Key,Value> skvi = clazz.getDeclaredConstructor().newInstance();

        Map<String,String> options = iteratorBuilder.iterOpts.get(iterInfo.iterName);

        if (options == null) {
          options = Collections.emptyMap();
        }

        skvi.init(prev, options, iteratorBuilder.iteratorEnvironment);
        prev = skvi;

      } catch (ReflectiveOperationException e) {
        log.error("Failed to load iterator {}, for table {}, from context {}", iterInfo.className,
            iteratorBuilder.iteratorEnvironment.getTableId(), iteratorBuilder.context, e);
        // This has to be a RuntimeException to be handled properly to fail the scan
        throw new RuntimeException(e);
      }
    }
    return prev;
  }

  private static Class<SortedKeyValueIterator<Key,Value>> loadClass(boolean useAccumuloClassLoader,
      String context, IterInfo iterInfo) throws ClassNotFoundException {
    if (useAccumuloClassLoader) {
      @SuppressWarnings("unchecked")
      var clazz = (Class<SortedKeyValueIterator<Key,Value>>) ClassLoaderUtil.loadClass(context,
          iterInfo.className, SortedKeyValueIterator.class);
      log.trace("Iterator class {} loaded from context {}, classloader: {}", iterInfo.className,
          context, clazz.getClassLoader());
      return clazz;
    }
    @SuppressWarnings("unchecked")
    var clazz = (Class<SortedKeyValueIterator<Key,Value>>) Class.forName(iterInfo.className)
        .asSubclass(SortedKeyValueIterator.class);
    log.trace("Iterator class {} loaded from classpath", iterInfo.className);
    return clazz;
  }

  public static void checkIteratorConflicts(Map<String,String> props, String property, String value)
      throws AccumuloException {
    if (Objects.equals(props.get(property), value)) {
      // setting a property that already exists (i.e., no change)
      return;
    }

    var iterProp = IteratorProperty.parse(property, value);
    if (iterProp != null && !iterProp.isOption()) {
      checkIteratorConflicts(props, iterProp.toSetting(), EnumSet.of(iterProp.getScope()), false);
    }
  }

  public static void checkIteratorConflicts(TableOperations tableOps, NamespaceOperationsHelper noh,
      String namespace, String property, String value)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    var props = noh.getNamespaceProperties(namespace);
    if (Objects.equals(props.get(property), value)) {
      // setting a property that already exists (i.e., no change)
      return;
    }

    var iterProp = IteratorProperty.parse(property, value);
    if (iterProp != null && !iterProp.isOption()) {
      // given a single property, the only way for the property to be equivalent to an existing
      // iterator is if the existing iterator has no options (opts are set as separate props)
      checkIteratorConflicts(props, iterProp.toSetting(), EnumSet.of(iterProp.getScope()), false);
    }

    // checking for conflicts for the tables in the namespace
    checkIteratorConflictsWithTablesInNamespace(tableOps, namespace, property, value);
  }

  public static void checkIteratorConflictsWithTablesInNamespace(TableOperations tableOps,
      String namespace, IteratorSetting is, EnumSet<IteratorScope> scopes)
      throws AccumuloException {
    Set<String> tablesInNamespace;
    if (namespace.equals(Namespace.DEFAULT.name())) {
      tablesInNamespace = tableOps.list().stream().filter(t -> !t.contains(Namespace.SEPARATOR))
          .collect(Collectors.toSet());
    } else {
      tablesInNamespace = tableOps.list().stream()
          .filter(t -> t.startsWith(namespace + Namespace.SEPARATOR)).collect(Collectors.toSet());
    }
    try {
      for (var table : tablesInNamespace) {
        checkIteratorConflicts(tableOps.getTableProperties(table), is, scopes, false);
      }
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
  }

  public static void checkIteratorConflictsWithTablesInNamespace(TableOperations tableOps,
      String namespace, String property, String value) throws AccumuloException {
    Set<String> tablesInNamespace;
    if (namespace.equals(Namespace.DEFAULT.name())) {
      tablesInNamespace = tableOps.list().stream().filter(t -> !t.contains(Namespace.SEPARATOR))
          .collect(Collectors.toSet());
    } else {
      tablesInNamespace = tableOps.list().stream()
          .filter(t -> t.startsWith(namespace + Namespace.SEPARATOR)).collect(Collectors.toSet());
    }
    try {
      for (var table : tablesInNamespace) {
        checkIteratorConflicts(tableOps.getTableProperties(table), property, value);
      }
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
  }

  public static void checkIteratorConflicts(IteratorSetting iterToCheck,
      EnumSet<IteratorScope> iterScopesToCheck,
      Map<IteratorScope,List<IteratorSetting>> existingIters, boolean shouldThrow)
      throws AccumuloException {
    // The reason for the 'shouldThrow' var is to prevent newly added 2.x checks from breaking
    // existing user code. Just log the problem and proceed. Major version > 2 will always throw
    for (var scope : iterScopesToCheck) {
      var existingItersForScope = existingIters.get(scope);
      if (existingItersForScope == null) {
        continue;
      }
      for (var existingIter : existingItersForScope) {
        // not a conflict if exactly the same
        if (iterToCheck.equals(existingIter)) {
          continue;
        }
        if (iterToCheck.getName().equals(existingIter.getName())) {
          String msg =
              String.format("iterator name conflict at %s scope. %s conflicts with existing %s",
                  scope, iterToCheck, existingIter);
          if (shouldThrow) {
            throw new AccumuloException(new IllegalArgumentException(msg));
          } else {
            log.warn(msg + WARNING_MSG);
          }
        }
        if (iterToCheck.getPriority() == existingIter.getPriority()) {
          String msg =
              String.format("iterator priority conflict at %s scope. %s conflicts with existing %s",
                  scope, iterToCheck, existingIter);
          if (shouldThrow) {
            throw new AccumuloException(new IllegalArgumentException(msg));
          } else {
            log.warn(msg + WARNING_MSG);
          }
        }
      }
    }
  }

  public static void checkIteratorConflicts(Map<String,String> props, IteratorSetting iterToCheck,
      EnumSet<IteratorScope> iterScopesToCheck, boolean shouldThrow) throws AccumuloException {
    // parse the props map
    Map<IteratorScope,Map<String,IteratorSetting>> iteratorSettings = new HashMap<>();
    Map<IteratorScope,List<IteratorSetting>> existingIters = new HashMap<>();

    for (var prop : props.entrySet()) {
      var iterProp = IteratorProperty.parse(prop.getKey(), prop.getValue());
      if (iterProp != null && !iterProp.isOption()
          && iterScopesToCheck.contains(iterProp.getScope())) {
        var iterSetting = iterProp.toSetting();
        iteratorSettings.computeIfAbsent(iterProp.getScope(), s -> new HashMap<>())
            .put(iterProp.getName(), iterSetting);
        existingIters.computeIfAbsent(iterProp.getScope(), s -> new ArrayList<>()).add(iterSetting);
      }
    }

    // check for conflicts
    // any iterator option property not part of an existing iterator is an option conflict
    for (var prop : props.entrySet()) {
      var iterProp = IteratorProperty.parse(prop.getKey(), prop.getValue());
      if (iterProp != null && iterProp.isOption()
          && iterScopesToCheck.contains(iterProp.getScope())) {
        var iterSetting =
            iteratorSettings.getOrDefault(iterProp.getScope(), Map.of()).get(iterProp.getName());
        if (iterSetting == null) {
          String msg = String.format("iterator options conflict for %s : %s=%s",
              iterToCheck.getName(), prop.getKey(), prop.getValue());
          if (shouldThrow) {
            throw new AccumuloException(new IllegalArgumentException(msg));
          } else {
            log.warn(msg + WARNING_MSG);
          }
        } else {
          iterSetting.addOption(iterProp.getOptionKey(), iterProp.getOptionValue());
        }
      }
    }
    // check if the given iterator conflicts with any existing iterators
    checkIteratorConflicts(iterToCheck, iterScopesToCheck, existingIters, shouldThrow);
  }

  /**
   * Returns a new map of all the iterator props contained in the given map
   */
  public static Map<String,String> gatherIteratorProps(Map<String,String> props) {
    Map<String,String> iterProps = new HashMap<>();
    for (var e : props.entrySet()) {
      var iterProp = IteratorProperty.parse(e.getKey(), e.getValue());
      if (iterProp != null) {
        iterProps.put(e.getKey(), e.getValue());
      }
    }
    return iterProps;
  }

  /**
   * returns a map of the options associated with the given iterator property key. Options of the
   * iterator are obtained by searching the given map
   */
  public static Map<String,String> gatherIterOpts(IteratorProperty nameProp,
      Map<String,String> map) {
    Map<String,String> opts = new HashMap<>();
    for (var e : map.entrySet()) {
      var iterProp = IteratorProperty.parse(e.getKey(), e.getValue());
      if (iterProp != null && iterProp.isOption() && nameProp.getName().equals(iterProp.getName())
          && nameProp.getScope().equals(iterProp.getScope())) {
        opts.put(iterProp.getOptionKey(), iterProp.getOptionValue());
      }
    }
    return opts;
  }
}
