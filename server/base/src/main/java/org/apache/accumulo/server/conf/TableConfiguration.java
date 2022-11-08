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
package org.apache.accumulo.server.conf;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfiguration extends ZooBasedConfiguration {

  private final static Logger log = LoggerFactory.getLogger(TableConfiguration.class);

  private final TableId tableId;

  private final EnumMap<IteratorScope,Deriver<ParsedIteratorConfig>> iteratorConfig;

  private final Deriver<ScanDispatcher> scanDispatchDeriver;
  private final Deriver<CompactionDispatcher> compactionDispatchDeriver;
  private final Deriver<CryptoService> cryptoServiceDeriver;

  public TableConfiguration(ServerContext context, TableId tableId, NamespaceConfiguration parent) {
    super(log, context, TablePropKey.of(context, tableId), parent);
    this.tableId = tableId;

    iteratorConfig = new EnumMap<>(IteratorScope.class);
    for (IteratorScope scope : IteratorScope.values()) {
      iteratorConfig.put(scope, newDeriver(conf -> {
        Map<String,Map<String,String>> allOpts = new HashMap<>();
        List<IterInfo> iters =
            IteratorConfigUtil.parseIterConf(scope, Collections.emptyList(), allOpts, conf);
        return new ParsedIteratorConfig(iters, allOpts, ClassLoaderUtil.tableContext(conf));
      }));
    }

    scanDispatchDeriver = newDeriver(conf -> createScanDispatcher(conf, context, tableId));
    compactionDispatchDeriver =
        newDeriver(conf -> createCompactionDispatcher(conf, context, tableId));
    cryptoServiceDeriver =
        newDeriver(conf -> createCryptoService(conf, tableId, context.getCryptoFactory()));
  }

  @Override
  public boolean isPropertySet(Property prop) {
    if (_isPropertySet(prop)) {
      return true;
    }

    return getParent().isPropertySet(prop);
  }

  private boolean _isPropertySet(Property property) {
    Map<String,String> propMap = getSnapshot();
    return propMap.get(property.getKey()) != null;
  }

  @Override
  public String get(Property property) {
    String value = _get(property);
    if (value != null) {
      return value;
    }
    AccumuloConfiguration parent = getParent();
    if (parent != null) {
      return parent.get(property);
    }
    return null;
  }

  @Nullable
  private String _get(Property property) {
    Map<String,String> propMap = getSnapshot();
    if (propMap == null) {
      return null;
    }
    return propMap.get(property.getKey());
  }

  public TableId getTableId() {
    return tableId;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  public static class ParsedIteratorConfig {
    private final List<IterInfo> tableIters;
    private final Map<String,Map<String,String>> tableOpts;
    private final String context;

    private ParsedIteratorConfig(List<IterInfo> ii, Map<String,Map<String,String>> opts,
        String context) {
      this.tableIters = List.copyOf(ii);
      tableOpts = opts.entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(Entry::getKey, e -> Map.copyOf(e.getValue())));
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

      private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return senv;
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

      private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return senv;
      }
    });

    return newDispatcher;
  }

  public ScanDispatcher getScanDispatcher() {
    return scanDispatchDeriver.derive();
  }

  public CompactionDispatcher getCompactionDispatcher() {
    return compactionDispatchDeriver.derive();
  }

  private CryptoService createCryptoService(AccumuloConfiguration conf, TableId tableId,
      CryptoServiceFactory factory) {
    CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.TABLE, tableId, null);
    return factory.getService(env, conf.getAllCryptoProperties());
  }

  public CryptoService getCryptoService() {
    return cryptoServiceDeriver.derive();
  }
}
