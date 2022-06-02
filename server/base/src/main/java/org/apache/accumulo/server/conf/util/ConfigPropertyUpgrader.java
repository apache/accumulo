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

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ConfigPropertyUpgrader implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(ConfigPropertyUpgrader.class);

  private final static VersionedPropCodec codec = VersionedPropCodec.getDefault();

  public ConfigPropertyUpgrader() {}

  public static void main(String[] args) throws Exception {
    new ConfigPropertyUpgrader().execute(args);
  }

  @Override
  public String keyword() {
    return "config-upgrade";
  }

  @Override
  public String description() {
    return "converts properties store in ZooKeeper to 2.1 format";
  }

  @Override
  public void execute(final String[] args) throws Exception {
    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(ConfigPropertyUpgrader.class.getName(), args);

    ServerContext context = opts.getServerContext();

    doUpgrade(context.getInstanceID(), context.getZooReaderWriter());
  }

  public void doUpgrade(final InstanceId instanceId, final ZooReaderWriter zrw) {

    ReadyMonitor readyMonitor = new ReadyMonitor(ConfigPropertyUpgrader.class.getSimpleName(),
        zrw.getSessionTimeout() * 2L);
    PropStoreWatcher nullWatcher = new PropStoreWatcher(readyMonitor);

    ConfigTransformer transformer = new ConfigTransformer(zrw, codec, nullWatcher);

    upgradeSysProps(instanceId, transformer);
    upgradeNamespaceProps(instanceId, zrw, transformer);
    upgradeTableProps(instanceId, zrw, transformer);
  }

  private void upgradeSysProps(final InstanceId instanceId, final ConfigTransformer transformer) {
    log.info("Upgrade system config properties for {}", instanceId);
    transformer.transform(SystemPropKey.of(instanceId));
  }

  private void upgradeNamespaceProps(final InstanceId instanceId, final ZooReaderWriter zrw,
      final ConfigTransformer transformer) {
    String zkPathNamespaceBase = ZooUtil.getRoot(instanceId) + Constants.ZNAMESPACES;
    try {
      // sort is cosmetic - only improves readability and consistency in logs
      Set<String> namespaces = new TreeSet<>(zrw.getChildren(zkPathNamespaceBase));
      for (String namespace : namespaces) {
        String zkPropBasePath = zkPathNamespaceBase + "/" + namespace + Constants.ZNAMESPACE_CONF;
        log.info("Upgrading namespace {} base path: {}", namespace, zkPropBasePath);
        transformer.transform(NamespacePropKey.of(instanceId, NamespaceId.of(namespace)));
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Failed to read namespaces from ZooKeeper for path: " + zkPathNamespaceBase, ex);
    } catch (InterruptedException ex) {
      throw new IllegalStateException(
          "Interrupted reading namespaces from ZooKeeper for path: " + zkPathNamespaceBase, ex);
    }
  }

  private void upgradeTableProps(final InstanceId instanceId, final ZooReaderWriter zrw,
      ConfigTransformer transformer) {
    String zkPathTableBase = ZooUtil.getRoot(instanceId) + Constants.ZTABLES;
    try {
      // sort is cosmetic - only improves readability and consistency in logs
      Set<String> tables = new TreeSet<>(zrw.getChildren(zkPathTableBase));
      for (String table : tables) {
        String zkPropBasePath = zkPathTableBase + "/" + table + Constants.ZNAMESPACE_CONF;
        log.info("Upgrading table {} base path: {}", table, zkPropBasePath);
        transformer.transform(TablePropKey.of(instanceId, TableId.of(table)));
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Failed to read tables from ZooKeeper for path: " + zkPathTableBase, ex);
    } catch (InterruptedException ex) {
      throw new IllegalStateException(
          "Interrupted reading tables from ZooKeeper for path: " + zkPathTableBase, ex);
    }
  }

}
