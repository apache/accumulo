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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO - this is in progress and should not be merged without changes.
// TODO - needs upgrade integration and testing.
/**
 * Convert pre-2.1 system, namespace and table properties to PropEncoded format.
 *
 * <pre>
 * Source ZooKeeper paths:
 *   srcSysPath - system config source = /accumulo/[iid-id]/config;
 *   srcNsBasePath - namespace config source /accumulo/[iid]/namespaces;
 *   srcTableBasePath - table config source /accumulo/[iid]/tables;
 * </pre>
 */
public class ConfigConverter {

  private static final Logger log = LoggerFactory.getLogger(ConfigConverter.class);

  private final ZooReaderWriter zrw;
  private final InstanceId instanceId;

  private final PropStore propStore;

  private final String zkBasePath; // base path for accumulo instance - /accumulo/[iid]

  private final Set<String> legacyPaths = new HashSet<>();

  public ConfigConverter(final ServerContext context) {

    instanceId = context.getInstanceID();
    zrw = context.getZooReaderWriter();
    propStore = context.getPropStore();

    zkBasePath = ZooUtil.getRoot(instanceId);
  }

  public synchronized static void convert(final ServerContext context,
      final boolean deleteWhenComplete) {
    ConfigConverter converter = new ConfigConverter(context);
    converter.convertSys();
    converter.convertNamespace();
    converter.convertTables();

    if (deleteWhenComplete) {
      converter.removeLegacyPaths();
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ConfigConverter.class.getSimpleName() + "[", "]")
        .add("converted=" + legacyPaths).toString();
  }

  public void convertSys() {
    var sysPropKey = PropCacheKey.forSystem(instanceId);
    var zkPathSysConfig = zkBasePath + Constants.ZCONFIG;

    Map<String,String> props = readLegacyProps(zkPathSysConfig);

    Map<String,String> renamedProps = new HashMap<>();
    props.forEach((original, value) -> {
      var finalName = DeprecatedPropertyUtil.getReplacementName(original,
          (log, replacement) -> log
              .info("Automatically renaming deprecated property '{}' with its replacement '{}'"
                  + " in ZooKeeper configuration upgrade.", original, replacement));
      renamedProps.put(finalName, value);
    });

    log.info("system props: {} -> {}", props, renamedProps);

    writeConverted(sysPropKey, renamedProps, zkPathSysConfig);

    // delete - the confirmation and then delete done in two steps so that the removal is atomic.
    // If the props were deleted as confirmed
  }

  public void convertNamespace() {
    var zkPathNamespaceBase = zkBasePath + Constants.ZNAMESPACES;
    try {
      List<String> namespaces = zrw.getChildren(zkPathNamespaceBase);
      for (String namespace : namespaces) {
        String zkPropBasePath = zkPathNamespaceBase + "/" + namespace + Constants.ZNAMESPACE_CONF;
        log.info("NS:{} base path: {}", namespace, zkPropBasePath);
        Map<String,String> props = readLegacyProps(zkPropBasePath);
        log.info("Namespace props: {} - {}", namespace, props);
        writeConverted(PropCacheKey.forNamespace(instanceId, NamespaceId.of(namespace)), props,
            zkPropBasePath);
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Failed to convert namespace from ZooKeeper for path: " + zkPathNamespaceBase, ex);
    } catch (InterruptedException ex) {
      throw new IllegalStateException(
          "Interrupted reading namespaces from ZooKeeper for path: " + zkPathNamespaceBase, ex);
    }
  }

  public void convertTables() {
    var zkPathTableBase = zkBasePath + Constants.ZTABLES;
    try {
      List<String> tables = zrw.getChildren(zkPathTableBase);
      for (String table : tables) {
        String zkPropBasePath = zkPathTableBase + "/" + table + Constants.ZTABLE_CONF;
        log.info("table:{} base path: {}", table, zkPropBasePath);
        Map<String,String> props = readLegacyProps(zkPropBasePath);
        log.info("table props: {} - {}", table, props);
        writeConverted(PropCacheKey.forTable(instanceId, TableId.of(table)), props, zkPropBasePath);
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Failed to convert tables from ZooKeeper for path: " + zkPathTableBase, ex);
    } catch (InterruptedException ex) {
      throw new IllegalStateException(
          "Interrupted reading namespaces from ZooKeeper for path: " + zkPathTableBase, ex);
    }
  }

  private void removeLegacyPaths() {
    for (String path : legacyPaths) {
      log.debug("delete ZooKeeper path: {}", path);
      try {
        zrw.delete(path);
      } catch (KeeperException ex) {
        log.warn(
            "Failed to delete path on property conversion " + path + ", reason" + ex.getMessage());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
    }
  }

  private Map<String,String> readLegacyProps(final String path) {
    requireNonNull(path, "A ZooKeeper path for configuration properties must be supplied");
    Map<String,String> props = new HashMap<>();
    try {
      List<String> children = zrw.getChildren(path);
      log.info("Looking in: {}, found: {}", path, children);
      for (String child : children) {
        if (Property.isValidPropertyKey(child)) {
          byte[] bytes = zrw.getData(path + "/" + child);
          props.put(child, new String(bytes, UTF_8));
          legacyPaths.add(path + "/" + child);
        } else {
          log.info("Skipping invalid property: {} in {}", child, path);
        }
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to get children from ZooKeeper for path: " + path,
          ex);
    } catch (InterruptedException ex) {
      throw new IllegalStateException(
          "Interrupted reading children from ZooKeeper for path: " + path, ex);
    }
    return props;
  }

  private void writeConverted(final PropCacheKey propCacheKey, final Map<String,String> props,
      final String legacyBasePath) {

    // will throw PropStoreException if create fails.
    propStore.create(propCacheKey, props);

    // confirm
    Map<String,String> confirmed = new TreeMap<>();
    Map<String,String> rejected = new TreeMap<>();
    VersionedProperties readSysProps = propStore.get(propCacheKey);
    if (readSysProps == null) {
      return;
    }
    for (Map.Entry<String,String> e : readSysProps.getProperties().entrySet()) {
      String propName = e.getKey();
      try {
        byte[] bytes = zrw.getData(legacyBasePath + "/" + propName);
        String readVal = new String(bytes, UTF_8);
        if (e.getValue().equals(readVal)) {
          confirmed.put(propName, readVal);
        } else {
          rejected.put(propName, readVal);
        }
      } catch (KeeperException ex) {
        throw new IllegalStateException(
            "Failed to read property: " + e.getKey() + " during conversion to: " + propCacheKey);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupt received conversing properties for " + propCacheKey);
      }
    }

    log.info("Converter results for id: {} - confirmed: {}, rejected: {}", propCacheKey, confirmed,
        rejected);

  }
}
