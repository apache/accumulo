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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for configuring iterators. These methods were moved from IteratorUtil so that it
 * could be treated as API.
 */
public class IterConfigUtil {
  private static final Logger log = LoggerFactory.getLogger(IterConfigUtil.class);

  public static Comparator<IterInfo> ITER_INFO_COMPARATOR = Comparator
      .comparingInt(IterInfo::getPriority);

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
   * @param limitVersion
   *          include a VersioningIterator at priority 20 that retains a single version of a given
   *          K/V pair.
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

  public static void parseIterConf(IteratorScope scope, List<IterInfo> iters,
      Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
    parseIterConf(scope, iters, allOptions, conf.getAllPropertiesWithPrefix(getProperty(scope)));
  }

  public static void parseIterConf(IteratorScope scope, List<IterInfo> iters,
      Map<String,Map<String,String>> allOptions, Map<String,String> properties) {
    final Property scopeProperty = getProperty(scope);
    final String scopePropertyKey = scopeProperty.getKey();

    for (Map.Entry<String,String> entry : properties.entrySet()) {
      String suffix = entry.getKey().substring(scopePropertyKey.length());
      String suffixSplit[] = suffix.split("\\.", 3);

      if (suffixSplit.length == 1) {
        String sa[] = entry.getValue().split(",");
        int prio = Integer.parseInt(sa[0]);
        String className = sa[1];
        iters.add(new IterInfo(prio, className, suffixSplit[0]));
      } else if (suffixSplit.length == 3 && suffixSplit[1].equals("opt")) {
        String iterName = suffixSplit[0];
        String optName = suffixSplit[2];

        Map<String,String> options = allOptions.get(iterName);
        if (options == null) {
          options = new HashMap<>();
          allOptions.put(iterName, options);
        }

        options.put(optName, entry.getValue());

      } else {
        throw new IllegalArgumentException("Invalid iterator format: " + entry.getKey());
      }
    }

    Collections.sort(iters, ITER_INFO_COMPARATOR);
  }

  public static void mergeIteratorConfig(List<IterInfo> destList,
      Map<String,Map<String,String>> destOpts, List<IterInfo> tableIters,
      Map<String,Map<String,String>> tableOpts, List<IterInfo> ssi,
      Map<String,Map<String,String>> ssio) {
    destList.addAll(tableIters);
    destList.addAll(ssi);
    Collections.sort(destList, IterConfigUtil.ITER_INFO_COMPARATOR);

    Set<Map.Entry<String,Map<String,String>>> es = tableOpts.entrySet();
    for (Map.Entry<String,Map<String,String>> entry : es) {
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
    for (Map.Entry<String,Map<String,String>> entry : ssio.entrySet()) {
      if (entry.getValue() == null)
        continue;
      Map<String,String> options = allOptions.get(entry.getKey());
      if (options == null) {
        allOptions.put(entry.getKey(), entry.getValue());
      } else {
        options.putAll(entry.getValue());
      }
    }
  }

  public static IterLoad loadIterConf(IteratorScope scope, List<IterInfo> iters,
      Map<String,Map<String,String>> iterOpts, AccumuloConfiguration conf) {
    Map<String,Map<String,String>> allOptions = new HashMap<>();
    IterConfigUtil.parseIterConf(scope, iters, allOptions, conf);
    mergeOptions(iterOpts, allOptions);
    return new IterLoad().iters(iters).iterOpts(allOptions);
  }

  public static SortedKeyValueIterator<Key,Value> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<Key,Value> source, AccumuloConfiguration conf,
      List<IteratorSetting> iterators, IteratorEnvironment env) throws IOException {

    List<IterInfo> ssiList = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();

    for (IteratorSetting is : iterators) {
      ssiList.add(new IterInfo(is.getPriority(), is.getIteratorClass(), is.getName()));
      ssio.put(is.getName(), is.getOptions());
    }

    IterLoad il = loadIterConf(scope, ssiList, ssio, conf);
    return loadIterators(source,
        il.iterEnv(env).useAccumuloClassLoader(true).context(conf.get(Property.TABLE_CLASSPATH)));
  }

  public static SortedKeyValueIterator<Key,Value> loadIterators(
      SortedKeyValueIterator<Key,Value> source, IterLoad il) throws IOException {
    return loadIterators(source, null, il);
  }

  public static SortedKeyValueIterator<Key,Value> loadIterators(
      SortedKeyValueIterator<Key,Value> source,
      Map<String,Class<? extends SortedKeyValueIterator<Key,Value>>> classCache, IterLoad iterLoad)
      throws IOException {
    // wrap the source in a SynchronizedIterator in case any of the additional configured iterators
    // want to use threading
    SortedKeyValueIterator<Key,Value> prev = source;

    try {
      for (IterInfo iterInfo : iterLoad.iters) {

        Class<? extends SortedKeyValueIterator<Key,Value>> clazz = null;
        log.trace("Attempting to load iterator class {}", iterInfo.className);
        if (classCache != null) {
          clazz = classCache.get(iterInfo.className);

          if (clazz == null) {
            clazz = loadClass(iterLoad.useAccumuloClassLoader, iterLoad.context, iterInfo);
            classCache.put(iterInfo.className, clazz);
          }
        } else {
          clazz = loadClass(iterLoad.useAccumuloClassLoader, iterLoad.context, iterInfo);
        }

        SortedKeyValueIterator<Key,Value> skvi = clazz.newInstance();

        Map<String,String> options = iterLoad.iterOpts.get(iterInfo.iterName);

        if (options == null)
          options = Collections.emptyMap();

        skvi.init(prev, options, iterLoad.iteratorEnvironment);
        prev = skvi;
      }
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      log.error(e.toString());
      throw new RuntimeException(e);
    }
    return prev;
  }

  @SuppressWarnings("unchecked")
  private static Class<SortedKeyValueIterator<Key,Value>> loadClass(boolean useAccumuloClassLoader,
      String context, IterInfo iterInfo) throws ClassNotFoundException, IOException {
    Class<SortedKeyValueIterator<Key,Value>> clazz;
    if (useAccumuloClassLoader) {
      if (context != null && !context.equals("")) {
        clazz = (Class<SortedKeyValueIterator<Key,Value>>) AccumuloVFSClassLoader
            .getContextManager()
            .loadClass(context, iterInfo.className, SortedKeyValueIterator.class);
        log.trace("Iterator class {} loaded from context {}, classloader: {}", iterInfo.className,
            context, clazz.getClassLoader());
      } else {
        clazz = (Class<SortedKeyValueIterator<Key,Value>>) AccumuloVFSClassLoader
            .loadClass(iterInfo.className, SortedKeyValueIterator.class);
        log.trace("Iterator class {} loaded from AccumuloVFSClassLoader: {}", iterInfo.className,
            clazz.getClassLoader());
      }
    } else {
      clazz = (Class<SortedKeyValueIterator<Key,Value>>) Class.forName(iterInfo.className)
          .asSubclass(SortedKeyValueIterator.class);
      log.trace("Iterator class {} loaded from classpath", iterInfo.className);
    }
    return clazz;
  }
}
