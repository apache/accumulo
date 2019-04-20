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
package org.apache.accumulo.server.conf;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.IterConfigUtil;
import org.apache.accumulo.core.conf.ObservableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.conf.ZooCachePropertyAccessor.PropCacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class TableConfiguration extends ObservableConfiguration {
  private static final Logger log = LoggerFactory.getLogger(TableConfiguration.class);

  private static final Map<PropCacheKey,ZooCache> propCaches = new java.util.HashMap<>();

  private final AtomicReference<ZooCachePropertyAccessor> propCacheAccessor =
      new AtomicReference<>();
  private final ServerContext context;
  private final NamespaceConfiguration parent;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  private final TableId tableId;

  private EnumMap<IteratorScope,AtomicReference<ParsedIteratorConfig>> iteratorConfig;

  public TableConfiguration(ServerContext context, TableId tableId, NamespaceConfiguration parent) {
    this.context = requireNonNull(context);
    this.tableId = requireNonNull(tableId);
    this.parent = requireNonNull(parent);

    iteratorConfig = new EnumMap<>(IteratorScope.class);
    for (IteratorScope scope : IteratorScope.values()) {
      iteratorConfig.put(scope, new AtomicReference<>(null));
    }
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private ZooCache getZooCache() {
    synchronized (propCaches) {
      PropCacheKey key = new PropCacheKey(context.getInstanceID(), tableId.canonical());
      ZooCache propCache = propCaches.get(key);
      if (propCache == null) {
        propCache = zcf.getZooCache(context.getZooKeepers(), context.getZooKeepersSessionTimeOut(),
            new TableConfWatcher(context));
        propCaches.put(key, propCache);
      }
      return propCache;
    }
  }

  private ZooCachePropertyAccessor getPropCacheAccessor() {
    // updateAndGet below always calls compare and set, so avoid if not null
    ZooCachePropertyAccessor zcpa = propCacheAccessor.get();
    if (zcpa != null)
      return zcpa;

    return propCacheAccessor
        .updateAndGet(pca -> pca == null ? new ZooCachePropertyAccessor(getZooCache()) : pca);
  }

  @Override
  public void addObserver(ConfigurationObserver co) {
    if (tableId == null) {
      String err = "Attempt to add observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    super.addObserver(co);
  }

  @Override
  public void removeObserver(ConfigurationObserver co) {
    if (tableId == null) {
      String err = "Attempt to remove observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    super.removeObserver(co);
  }

  private String getPath() {
    return context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    if (!cacheAndWatch)
      throw new UnsupportedOperationException(
          "Table configuration only supports checking if a property is set in cache.");

    if (getPropCacheAccessor().isPropertySet(prop, getPath()))
      return true;

    return parent.isPropertySet(prop, cacheAndWatch);
  }

  @Override
  public String get(Property property) {
    return getPropCacheAccessor().get(property, getPath(), parent);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    getPropCacheAccessor().getProperties(props, getPath(), filter, parent, null);
  }

  public TableId getTableId() {
    return tableId;
  }

  /**
   * returns the actual NamespaceConfiguration that corresponds to the current parent namespace.
   */
  public NamespaceConfiguration getNamespaceConfiguration() {
    return context.getServerConfFactory().getNamespaceConfiguration(parent.namespaceId);
  }

  /**
   * Gets the parent configuration of this configuration.
   *
   * @return parent configuration
   */
  public NamespaceConfiguration getParentConfiguration() {
    return parent;
  }

  @Override
  public synchronized void invalidateCache() {
    ZooCachePropertyAccessor pca = propCacheAccessor.get();

    if (pca != null) {
      pca.invalidateCache();
    }
    // Else, if the accessor is null, we could lock and double-check
    // to see if it happened to be created so we could invalidate its cache
    // but I don't see much benefit coming from that extra check.
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  @Override
  public long getUpdateCount() {
    return parent.getUpdateCount() + getPropCacheAccessor().getZooCache().getUpdateCount();
  }

  public static class ParsedIteratorConfig {
    private final List<IterInfo> tableIters;
    private final Map<String,Map<String,String>> tableOpts;
    private final String context;
    private final long updateCount;

    private ParsedIteratorConfig(List<IterInfo> ii, Map<String,Map<String,String>> opts,
        String context, long updateCount) {
      this.tableIters = ImmutableList.copyOf(ii);
      Builder<String,Map<String,String>> imb = ImmutableMap.builder();
      for (Entry<String,Map<String,String>> entry : opts.entrySet()) {
        imb.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
      }
      tableOpts = imb.build();
      this.context = context;
      this.updateCount = updateCount;
    }

    public List<IterInfo> getIterInfo() {
      return tableIters;
    }

    public Map<String,Map<String,String>> getOpts() {
      return tableOpts;
    }

    public String getServiceEnv() {
      return context;
    }
  }

  public ParsedIteratorConfig getParsedIteratorConfig(IteratorScope scope) {
    long count = getUpdateCount();
    AtomicReference<ParsedIteratorConfig> ref = iteratorConfig.get(scope);
    ParsedIteratorConfig pic = ref.get();
    if (pic == null || pic.updateCount != count) {
      Map<String,Map<String,String>> allOpts = new HashMap<>();
      List<IterInfo> iters =
          IterConfigUtil.parseIterConf(scope, Collections.emptyList(), allOpts, this);
      ParsedIteratorConfig newPic =
          new ParsedIteratorConfig(iters, allOpts, get(Property.TABLE_CLASSPATH), count);
      ref.compareAndSet(pic, newPic);
      pic = newPic;
    }

    return pic;
  }

  public static class TablesScanDispatcher {
    public final ScanDispatcher dispatcher;
    public final long count;

    public TablesScanDispatcher(ScanDispatcher dispatcher, long count) {
      this.dispatcher = dispatcher;
      this.count = count;
    }
  }

  private AtomicReference<TablesScanDispatcher> scanDispatcherRef = new AtomicReference<>();

  public ScanDispatcher getScanDispatcher() {
    long count = getUpdateCount();
    TablesScanDispatcher currRef = scanDispatcherRef.get();
    if (currRef == null || currRef.count != count) {
      ScanDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(this,
          Property.TABLE_SCAN_DISPATCHER, ScanDispatcher.class, null);

      Builder<String,String> builder = ImmutableMap.builder();
      getAllPropertiesWithPrefix(Property.TABLE_SCAN_DISPATCHER_OPTS).forEach((k, v) -> {
        String optKey = k.substring(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey().length());
        builder.put(optKey, v);
      });

      Map<String,String> opts = builder.build();

      newDispatcher.init(new ScanDispatcher.InitParameters() {
        @Override
        public TableId getTableId() {
          return tableId;
        }

        @Override
        public Map<String,String> getOptions() {
          return opts;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
          return new ServiceEnvironmentImpl(context);
        }
      });

      TablesScanDispatcher newRef = new TablesScanDispatcher(newDispatcher, count);
      scanDispatcherRef.compareAndSet(currRef, newRef);
      currRef = newRef;
    }

    return currRef.dispatcher;
  }
}
