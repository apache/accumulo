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
package org.apache.accumulo.core.iterators;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TIteratorSetting;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IteratorUtil {

  private static final Logger log = LoggerFactory.getLogger(IteratorUtil.class);

  /**
   * Even though this type is not in a public API package, its used by methods in the public API. Therefore it should be treated as public API and should not
   * reference any non public API types. Also this type can not be moved.
   */
  public static enum IteratorScope {
    majc, minc, scan;
  }

  public static class IterInfoComparator implements Comparator<IterInfo>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(IterInfo o1, IterInfo o2) {
      return (o1.priority < o2.priority ? -1 : (o1.priority == o2.priority ? 0 : 1));
    }

  }

  /**
   * Fetch the correct configuration key prefix for the given scope. Throws an IllegalArgumentException if no property exists for the given scope.
   */
  static Property getProperty(IteratorScope scope) {
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
   *          include a VersioningIterator at priority 20 that retains a single version of a given K/V pair.
   * @return A map of Table properties
   */
  public static Map<String,String> generateInitialTableProperties(boolean limitVersion) {
    TreeMap<String,String> props = new TreeMap<>();

    if (limitVersion) {
      for (IteratorScope iterScope : IteratorScope.values()) {
        props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers", "20," + VersioningIterator.class.getName());
        props.put(Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions", "1");
      }
    }

    props.put(Property.TABLE_CONSTRAINT_PREFIX.toString() + "1", DefaultKeySizeConstraint.class.getName());

    return props;
  }

  public static void mergeIteratorConfig(List<IterInfo> destList, Map<String,Map<String,String>> destOpts, List<IterInfo> tableIters,
      Map<String,Map<String,String>> tableOpts, List<IterInfo> ssi, Map<String,Map<String,String>> ssio) {
    destList.addAll(tableIters);
    destList.addAll(ssi);
    Collections.sort(destList, new IterInfoComparator());

    Set<Entry<String,Map<String,String>>> es = tableOpts.entrySet();
    for (Entry<String,Map<String,String>> entry : es) {
      if (entry.getValue() == null) {
        destOpts.put(entry.getKey(), null);
      } else {
        destOpts.put(entry.getKey(), new HashMap<>(entry.getValue()));
      }
    }

    IteratorUtil.mergeOptions(ssio, destOpts);

  }

  public static void parseIterConf(IteratorScope scope, List<IterInfo> iters, Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
    final Property scopeProperty = getProperty(scope);
    final String scopePropertyKey = scopeProperty.getKey();

    for (Entry<String,String> entry : conf.getAllPropertiesWithPrefix(scopeProperty).entrySet()) {
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
        log.warn("Unrecognizable option: {}", entry.getKey());
      }
    }

    Collections.sort(iters, new IterInfoComparator());
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, IteratorEnvironment env) throws IOException {
    List<IterInfo> emptyList = Collections.emptyList();
    Map<String,Map<String,String>> emptyMap = Collections.emptyMap();
    return loadIterators(scope, source, extent, conf, emptyList, emptyMap, env);
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IteratorSetting> iterators, IteratorEnvironment env)
      throws IOException {

    List<IterInfo> ssiList = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();

    for (IteratorSetting is : iterators) {
      ssiList.add(new IterInfo(is.getPriority(), is.getIteratorClass(), is.getName()));
      ssio.put(is.getName(), is.getOptions());
    }

    return loadIterators(scope, source, extent, conf, ssiList, ssio, env, true);
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      IteratorEnvironment env) throws IOException {
    return loadIterators(scope, source, extent, conf, ssiList, ssio, env, true);
  }

  private static void parseIteratorConfiguration(IteratorScope scope, List<IterInfo> iters, Map<String,Map<String,String>> ssio,
      Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
    parseIterConf(scope, iters, allOptions, conf);

    mergeOptions(ssio, allOptions);
  }

  private static void mergeOptions(Map<String,Map<String,String>> ssio, Map<String,Map<String,String>> allOptions) {
    for (Entry<String,Map<String,String>> entry : ssio.entrySet()) {
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

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      IteratorEnvironment env, boolean useAccumuloClassLoader) throws IOException {

    return loadIteratorsHelper(scope, source, extent, conf, ssiList, ssio, env, useAccumuloClassLoader, conf.get(Property.TABLE_CLASSPATH));
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      IteratorEnvironment env, boolean useAccumuloClassLoader, String classLoaderContext) throws IOException {

    return loadIteratorsHelper(scope, source, extent, conf, ssiList, ssio, env, useAccumuloClassLoader, classLoaderContext);
  }

  private static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIteratorsHelper(IteratorScope scope,
      SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      IteratorEnvironment env, boolean useAccumuloClassLoader, String classLoaderContext) throws IOException {

    List<IterInfo> iters = new ArrayList<>(ssiList);
    Map<String,Map<String,String>> allOptions = new HashMap<>();
    parseIteratorConfiguration(scope, iters, ssio, allOptions, conf);
    return loadIterators(source, iters, allOptions, env, useAccumuloClassLoader, classLoaderContext);
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(SortedKeyValueIterator<K,V> source,
      Collection<IterInfo> iters, Map<String,Map<String,String>> iterOpts, IteratorEnvironment env, boolean useAccumuloClassLoader, String context)
      throws IOException {
    return loadIterators(source, iters, iterOpts, env, useAccumuloClassLoader, context, null);
  }

  public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(SortedKeyValueIterator<K,V> source,
      Collection<IterInfo> iters, Map<String,Map<String,String>> iterOpts, IteratorEnvironment env, boolean useAccumuloClassLoader, String context,
      Map<String,Class<? extends SortedKeyValueIterator<K,V>>> classCache) throws IOException {
    // wrap the source in a SynchronizedIterator in case any of the additional configured iterators want to use threading
    SortedKeyValueIterator<K,V> prev = source;

    try {
      for (IterInfo iterInfo : iters) {

        Class<? extends SortedKeyValueIterator<K,V>> clazz = null;
        log.trace("Attempting to load iterator class {}", iterInfo.className);
        if (classCache != null) {
          clazz = classCache.get(iterInfo.className);

          if (clazz == null) {
            clazz = loadClass(useAccumuloClassLoader, context, iterInfo);
            classCache.put(iterInfo.className, clazz);
          }
        } else {
          clazz = loadClass(useAccumuloClassLoader, context, iterInfo);
        }

        SortedKeyValueIterator<K,V> skvi = clazz.newInstance();

        Map<String,String> options = iterOpts.get(iterInfo.iterName);

        if (options == null)
          options = Collections.emptyMap();

        skvi.init(prev, options, env);
        prev = skvi;
      }
    } catch (ClassNotFoundException e) {
      log.error(e.toString());
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      log.error(e.toString());
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      log.error(e.toString());
      throw new RuntimeException(e);
    }
    return prev;
  }

  @SuppressWarnings("unchecked")
  private static <K extends WritableComparable<?>,V extends Writable> Class<? extends SortedKeyValueIterator<K,V>> loadClass(boolean useAccumuloClassLoader,
      String context, IterInfo iterInfo) throws ClassNotFoundException, IOException {
    Class<? extends SortedKeyValueIterator<K,V>> clazz;
    if (useAccumuloClassLoader) {
      if (context != null && !context.equals("")) {
        clazz = (Class<? extends SortedKeyValueIterator<K,V>>) AccumuloVFSClassLoader.getContextManager().loadClass(context, iterInfo.className,
            SortedKeyValueIterator.class);
        log.trace("Iterator class {} loaded from context {}, classloader: {}", iterInfo.className, context, clazz.getClassLoader());
      } else {
        clazz = (Class<? extends SortedKeyValueIterator<K,V>>) AccumuloVFSClassLoader.loadClass(iterInfo.className, SortedKeyValueIterator.class);
        log.trace("Iterator class {} loaded from AccumuloVFSClassLoader: {}", iterInfo.className, clazz.getClassLoader());
      }
    } else {
      clazz = (Class<? extends SortedKeyValueIterator<K,V>>) Class.forName(iterInfo.className).asSubclass(SortedKeyValueIterator.class);
      log.trace("Iterator class {} loaded from classpath", iterInfo.className);
    }
    return clazz;
  }

  public static Range maximizeStartKeyTimeStamp(Range range) {
    Range seekRange = range;

    if (range.getStartKey() != null) {
      Key seekKey = range.getStartKey();
      if (range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
        seekKey = new Key(seekRange.getStartKey());
        seekKey.setTimestamp(Long.MAX_VALUE);
        seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
      } else if (!range.isStartKeyInclusive()) {
        seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
      }
    }

    return seekRange;
  }

  public static Range minimizeEndKeyTimeStamp(Range range) {
    Range seekRange = range;

    if (range.getEndKey() != null) {
      Key seekKey = seekRange.getEndKey();
      if (range.getEndKey().getTimestamp() != Long.MIN_VALUE) {
        seekKey = new Key(seekRange.getEndKey());
        seekKey.setTimestamp(Long.MIN_VALUE);
        seekRange = new Range(range.getStartKey(), range.isStartKeyInclusive(), seekKey, true);
      } else if (!range.isEndKeyInclusive()) {
        seekRange = new Range(range.getStartKey(), range.isStartKeyInclusive(), seekKey, true);
      }
    }

    return seekRange;
  }

  public static TIteratorSetting toTIteratorSetting(IteratorSetting is) {
    return new TIteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
  }

  public static IteratorSetting toIteratorSetting(TIteratorSetting tis) {
    return new IteratorSetting(tis.getPriority(), tis.getName(), tis.getIteratorClass(), tis.getProperties());
  }

  public static IteratorConfig toIteratorConfig(List<IteratorSetting> iterators) {
    ArrayList<TIteratorSetting> tisList = new ArrayList<>();

    for (IteratorSetting iteratorSetting : iterators) {
      tisList.add(toTIteratorSetting(iteratorSetting));
    }

    return new IteratorConfig(tisList);
  }

  public static List<IteratorSetting> toIteratorSettings(IteratorConfig ic) {
    List<IteratorSetting> ret = new ArrayList<>();
    for (TIteratorSetting tIteratorSetting : ic.getIterators()) {
      ret.add(toIteratorSetting(tIteratorSetting));
    }

    return ret;
  }

  public static byte[] encodeIteratorSettings(IteratorConfig iterators) {
    TSerializer tser = new TSerializer(new TBinaryProtocol.Factory());

    try {
      return tser.serialize(iterators);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encodeIteratorSettings(List<IteratorSetting> iterators) {
    return encodeIteratorSettings(toIteratorConfig(iterators));
  }

  public static List<IteratorSetting> decodeIteratorSettings(byte[] enc) {
    TDeserializer tdser = new TDeserializer(new TBinaryProtocol.Factory());
    IteratorConfig ic = new IteratorConfig();
    try {
      tdser.deserialize(ic, enc);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return toIteratorSettings(ic);
  }

  public static SortedKeyValueIterator<Key,Value> setupSystemScanIterators(SortedKeyValueIterator<Key,Value> source, Set<Column> cols, Authorizations auths,
      byte[] defaultVisibility) throws IOException {
    DeletingIterator delIter = new DeletingIterator(source, false);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);
    SortedKeyValueIterator<Key,Value> colFilter = ColumnQualifierFilter.wrap(cfsi, cols);
    return VisibilityFilter.wrap(colFilter, auths, defaultVisibility);
  }
}
