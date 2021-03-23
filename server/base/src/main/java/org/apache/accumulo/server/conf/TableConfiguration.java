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
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.IterConfigUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.conf.ZooCachePropertyAccessor.PropCacheKey;

import com.google.common.collect.ImmutableMap;

public class TableConfiguration extends AccumuloConfiguration {

  private static final Map<PropCacheKey,ZooCache> propCaches = new java.util.HashMap<>();

  private final AtomicReference<ZooCachePropertyAccessor> propCacheAccessor =
      new AtomicReference<>();
  private final ServerContext context;
  private final NamespaceConfiguration parent;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  private final TableId tableId;

  private final EnumMap<IteratorScope,Deriver<ParsedIteratorConfig>> iteratorConfig;

  private final Deriver<ScanDispatcher> scanDispatchDeriver;
  private final Deriver<CompactionDispatcher> compactionDispatchDeriver;
  private final Deriver<List<CryptoService>> decryptersDeriver;

  public TableConfiguration(ServerContext context, TableId tableId, NamespaceConfiguration parent) {
    this.context = requireNonNull(context);
    this.tableId = requireNonNull(tableId);
    this.parent = requireNonNull(parent);

    iteratorConfig = new EnumMap<>(IteratorScope.class);
    for (IteratorScope scope : IteratorScope.values()) {
      iteratorConfig.put(scope, newDeriver(conf -> {
        Map<String,Map<String,String>> allOpts = new HashMap<>();
        List<IterInfo> iters =
            IterConfigUtil.parseIterConf(scope, Collections.emptyList(), allOpts, conf);
        return new ParsedIteratorConfig(iters, allOpts, ClassLoaderUtil.tableContext(conf));
      }));
    }

    scanDispatchDeriver = newDeriver(conf -> createScanDispatcher(conf, context, tableId));
    compactionDispatchDeriver =
        newDeriver(conf -> createCompactionDispatcher(conf, context, tableId));
    decryptersDeriver = newDeriver(TableConfiguration::createDecryptersDeriver);
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private ZooCache getZooCache() {
    synchronized (propCaches) {
      PropCacheKey key = new PropCacheKey(context.getInstanceID(), tableId.canonical());
      ZooCache propCache = propCaches.get(key);
      if (propCache == null) {
        propCache = zcf.getZooCache(context.getZooKeepers(), context.getZooKeepersSessionTimeOut());
        propCaches.put(key, propCache);
      }
      return propCache;
    }
  }

  private ZooCachePropertyAccessor getPropCacheAccessor() {
    // updateAndGet below always calls compare and set, so avoid if not null
    ZooCachePropertyAccessor zcpa = propCacheAccessor.get();
    if (zcpa != null) {
      return zcpa;
    }

    return propCacheAccessor
        .updateAndGet(pca -> pca == null ? new ZooCachePropertyAccessor(getZooCache()) : pca);
  }

  private String getPath() {
    return context.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    if (!cacheAndWatch) {
      throw new UnsupportedOperationException(
          "Table configuration only supports checking if a property is set in cache.");
    }

    if (getPropCacheAccessor().isPropertySet(prop, getPath())) {
      return true;
    }

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

    private ParsedIteratorConfig(List<IterInfo> ii, Map<String,Map<String,String>> opts,
        String context) {
      this.tableIters = List.copyOf(ii);
      var imb = ImmutableMap.<String,Map<String,String>>builder();
      for (Entry<String,Map<String,String>> entry : opts.entrySet()) {
        imb.put(entry.getKey(), Map.copyOf(entry.getValue()));
      }
      tableOpts = imb.build();
      this.context = context;
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
    return iteratorConfig.get(scope).derive();
  }

  private static ScanDispatcher createScanDispatcher(AccumuloConfiguration conf,
      ServerContext context, TableId tableId) {
    ScanDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
        Property.TABLE_SCAN_DISPATCHER, ScanDispatcher.class, null);

    Map<String,String> opts =
        conf.getAllPropertiesWithPrefixStripped(Property.TABLE_SCAN_DISPATCHER_OPTS);

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

    return newDispatcher;
  }

  private static CompactionDispatcher createCompactionDispatcher(AccumuloConfiguration conf,
      ServerContext context, TableId tableId) {
    CompactionDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
        Property.TABLE_COMPACTION_DISPATCHER, CompactionDispatcher.class, null);

    Map<String,String> opts =
        conf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_DISPATCHER_OPTS);

    newDispatcher.init(new CompactionDispatcher.InitParameters() {
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

    return newDispatcher;
  }

  private static List<CryptoService> createDecryptersDeriver(AccumuloConfiguration conf) {
    return CryptoServiceFactory.getDecrypters(conf, CryptoServiceFactory.ClassloaderType.ACCUMULO);
  }

  public ScanDispatcher getScanDispatcher() {
    return scanDispatchDeriver.derive();
  }

  public CompactionDispatcher getCompactionDispatcher() {
    return compactionDispatchDeriver.derive();
  }

  public List<CryptoService> getDecrypters() {
    return decryptersDeriver.derive();
  }
}
