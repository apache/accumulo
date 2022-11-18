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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.IteratorSetting;
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
   * Generate the initial (default) properties for a table
   *
   * @param limitVersion include a VersioningIterator at priority 20 that retains a single version
   *        of a given K/V pair.
   * @return A map of Table properties
   */
  public static Map<String,String> generateInitialTableProperties(boolean limitVersion) {
    TreeMap<String,String> props = new TreeMap<>();

    if (limitVersion) {
      for (IteratorScope iterScope : IteratorScope.values()) {
        props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers",
            "20," + VersioningIterator.class.getName());
        props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions", "1");
      }
    }

    props.put(Property.TABLE_CONSTRAINT_PREFIX + "1", DefaultKeySizeConstraint.class.getName());

    return props;
  }

  public static List<IterInfo> parseIterConf(IteratorScope scope, List<IterInfo> iters,
      Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
    Map<String,String> properties = conf.getAllPropertiesWithPrefix(getProperty(scope));
    ArrayList<IterInfo> iterators = new ArrayList<>(iters);
    final Property scopeProperty = getProperty(scope);
    final String scopePropertyKey = scopeProperty.getKey();

    for (Entry<String,String> entry : properties.entrySet()) {
      String suffix = entry.getKey().substring(scopePropertyKey.length());
      String[] suffixSplit = suffix.split("\\.", 3);

      if (suffixSplit.length == 1) {
        String[] sa = entry.getValue().split(",");
        int prio = Integer.parseInt(sa[0]);
        String className = sa[1];
        iterators.add(new IterInfo(prio, className, suffixSplit[0]));
      } else if (suffixSplit.length == 3 && suffixSplit[1].equals("opt")) {
        String iterName = suffixSplit[0];
        String optName = suffixSplit[2];
        allOptions.computeIfAbsent(iterName, k -> new HashMap<>()).put(optName, entry.getValue());
      } else {
        throw new IllegalArgumentException("Invalid iterator format: " + entry.getKey());
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
      List<IteratorSetting> iterators, IteratorEnvironment env) throws IOException {

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
          throws IOException {
    SortedKeyValueIterator<Key,Value> prev = source;
    final boolean useClassLoader = iteratorBuilder.useAccumuloClassLoader;
    Map<String,Class<SortedKeyValueIterator<Key,Value>>> classCache = new HashMap<>();

    try {
      for (IterInfo iterInfo : iteratorBuilder.iters) {

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
      }
    } catch (ReflectiveOperationException e) {
      log.error(e.toString());
      throw new RuntimeException(e);
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
}
