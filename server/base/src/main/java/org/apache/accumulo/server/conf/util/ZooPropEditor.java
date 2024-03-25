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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.conf.util.ZooPropUtils.getNamespaceIdToNameMap;
import static org.apache.accumulo.server.conf.util.ZooPropUtils.getTableIdToName;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerInfo.ServerType;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ZooPropEditor implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropEditor.class);
  private final NullWatcher nullWatcher =
      new NullWatcher(new ReadyMonitor(ZooInfoViewer.class.getSimpleName(), 20_000L));

  /**
   * No-op constructor - provided so ServiceLoader autoload does not consume resources.
   */
  public ZooPropEditor() {}

  @Override
  public String keyword() {
    return "zoo-prop-editor";
  }

  @Override
  public String description() {
    return "Emergency tool to modify properties stored in ZooKeeper without a cluster."
        + " Prefer using the shell if it is available";
  }

  @Override
  public void execute(String[] args) throws Exception {
    ZooPropEditor.Opts opts = new ZooPropEditor.Opts();
    opts.parseArgs(ZooPropEditor.class.getName(), args);

    ZooReaderWriter zrw = new ZooReaderWriter(opts.getSiteConfiguration());

    var siteConfig = opts.getSiteConfiguration();
    try (ServerContext context = new ServerContext(ServerType.UTILITY, siteConfig)) {

      InstanceId iid = context.getInstanceID();

      PropStoreKey<?> propKey = getPropKey(iid, opts, zrw);
      switch (opts.getCmdMode()) {
        case SET:
          setProperty(context, propKey, opts);
          break;
        case DELETE:
          deleteProperty(context, propKey, readPropNode(propKey, zrw), opts);
          break;
        case PRINT:
          printProperties(context, propKey, readPropNode(propKey, zrw));
          break;
        case ERROR:
        default:
          throw new IllegalArgumentException("Invalid operation requested");
      }
    }
  }

  private void setProperty(final ServerContext context, final PropStoreKey<?> propKey,
      final Opts opts) {
    LOG.trace("set {}", propKey);

    if (!opts.setOpt.contains("=")) {
      throw new IllegalArgumentException(
          "Invalid set property format. Requires name=value, received " + opts.setOpt);
    }
    String targetName = "'invalid'";
    try {
      targetName = getDisplayName(propKey, context.getInstanceID(), context.getZooReader());

      Map<String,String> prev = context.getPropStore().get(propKey).asMap();
      String[] tokens = opts.setOpt.split("=");
      String propName = tokens[0].trim();
      String propVal = tokens[1].trim();
      Map<String,String> propMap = Map.of(propName, propVal);
      PropUtil.setProperties(context, propKey, propMap);

      if (prev.containsKey(propName)) {
        LOG.info("{}: modified {} from {} to {}", targetName, propName, prev.get(propName),
            propVal);
      } else {
        LOG.info("{}: set {}={}", targetName, propName, propVal);
      }
    } catch (Exception ex) {
      throw new IllegalStateException(
          "Failed to set property for " + targetName + " (id: " + propKey.getId() + ")", ex);
    }
  }

  private void deleteProperty(final ServerContext context, final PropStoreKey<?> propKey,
      VersionedProperties versionedProperties, final Opts opts) {
    LOG.trace("delete {} - {}", propKey, opts.deleteOpt);
    String p = opts.deleteOpt.trim();
    if (p.isEmpty() || !Property.isValidPropertyKey(p)) {
      throw new IllegalArgumentException("Invalid property name, Received: '" + p + "'");
    }
    // warn, but not throwing an error. If this was run in a script, allow the script to continue.
    if (!versionedProperties.asMap().containsKey(p)) {
      LOG.warn("skipping delete: property '{}' is not set for: {}- delete would have no effect", p,
          propKey);
      return;
    }
    PropUtil.removeProperties(context, propKey, List.of(p));
    String targetName = getDisplayName(propKey, context.getInstanceID(), context.getZooReader());
    LOG.info("{}: deleted {}", targetName, p);
  }

  private void printProperties(final ServerContext context, final PropStoreKey<?> propKey,
      final VersionedProperties props) {
    LOG.trace("print {}", propKey);

    OutputStream outStream = System.out;

    String scope;
    if (propKey instanceof SystemPropKey) {
      scope = "SYSTEM";
    } else if (propKey instanceof NamespacePropKey) {
      scope = "NAMESPACE";
    } else if (propKey instanceof TablePropKey) {
      scope = "TABLE";
    } else {
      scope = "unknown";
    }

    try (PrintWriter writer =
        new PrintWriter(new BufferedWriter(new OutputStreamWriter(outStream, UTF_8)))) {
      // header
      writer.printf(": Instance name: %s\n", context.getInstanceName());
      writer.printf(": Instance Id: %s\n", context.getInstanceID());
      writer.printf(": Property scope: %s\n", scope);
      writer.printf(": ZooKeeper path: %s\n", propKey.getPath());
      writer.printf(": Name: %s\n",
          getDisplayName(propKey, context.getInstanceID(), context.getZooReader()));
      writer.printf(": Id: %s\n", propKey.getId());
      writer.printf(": Data version: %d\n", props.getDataVersion());
      writer.printf(": Timestamp: %s\n", props.getTimestampISO());

      // skip filtering if no props
      if (props.asMap().isEmpty()) {
        return;
      }

      SortedMap<String,String> sortedMap = new TreeMap<>(props.asMap());
      sortedMap.forEach((name, value) -> writer.printf("%s=%s\n", name, value));
    }
  }

  private VersionedProperties readPropNode(final PropStoreKey<?> propKey,
      final ZooReader zooReader) {
    try {
      return ZooPropStore.readFromZk(propKey, nullWatcher, zooReader);
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private PropStoreKey<?> getPropKey(final InstanceId iid, final ZooPropEditor.Opts opts,
      final ZooReader zooReader) {

    // either tid or table name option provided, get the table id
    if (!opts.tableOpt.isEmpty() || !opts.tableIdOpt.isEmpty()) {
      TableId tid = getTableId(iid, opts, zooReader);
      return TablePropKey.of(iid, tid);
    }

    // either nid of namespace name provided, get the namespace id.
    if (!opts.namespaceOpt.isEmpty() || !opts.namespaceIdOpt.isEmpty()) {
      NamespaceId nid = getNamespaceId(iid, opts, zooReader);
      return NamespacePropKey.of(iid, nid);
    }

    // no table or namespace, assume system.
    return SystemPropKey.of(iid);
  }

  private TableId getTableId(final InstanceId iid, final ZooPropEditor.Opts opts,
      final ZooReader zooReader) {
    if (!opts.tableIdOpt.isEmpty()) {
      return TableId.of(opts.tableIdOpt);
    }
    Map<NamespaceId,String> nids = getNamespaceIdToNameMap(iid, zooReader);

    Map<TableId,String> tids = getTableIdToName(iid, nids, zooReader);
    return tids.entrySet().stream().filter(entry -> opts.tableOpt.equals(entry.getValue()))
        .map(Map.Entry::getKey).findAny()
        .orElseThrow(() -> new IllegalArgumentException("Could not find table " + opts.tableOpt));
  }

  private NamespaceId getNamespaceId(final InstanceId iid, final ZooPropEditor.Opts opts,
      final ZooReader zooReader) {
    if (!opts.namespaceIdOpt.isEmpty()) {
      return NamespaceId.of(opts.namespaceIdOpt);
    }
    Map<NamespaceId,String> nids = getNamespaceIdToNameMap(iid, zooReader);
    return nids.entrySet().stream().filter(entry -> opts.namespaceOpt.equals(entry.getValue()))
        .map(Map.Entry::getKey).findAny().orElseThrow(
            () -> new IllegalArgumentException("Could not find namespace " + opts.namespaceOpt));
  }

  private String getDisplayName(final PropStoreKey<?> propStoreKey, final InstanceId iid,
      final ZooReader zooReader) {

    if (propStoreKey instanceof TablePropKey) {
      Map<NamespaceId,String> nids = getNamespaceIdToNameMap(iid, zooReader);
      return getTableIdToName(iid, nids, zooReader).getOrDefault((TableId) propStoreKey.getId(),
          "unknown");
    }
    if (propStoreKey instanceof NamespacePropKey) {
      return getNamespaceIdToNameMap(iid, zooReader)
          .getOrDefault((NamespaceId) propStoreKey.getId(), "unknown");
    }
    if (propStoreKey instanceof SystemPropKey) {
      return "system";
    }
    LOG.info("Undefined PropStoreKey type provided, cannot decode name for classname: {}",
        propStoreKey.getClass().getName());
    return "unknown";
  }

  static class Opts extends ConfigOpts {

    @Parameter(names = {"-d", "--delete"}, description = "delete a property")
    public String deleteOpt = "";
    @Parameter(names = {"-ns", "--namespace"},
        description = "namespace to display/set/delete properties for")
    public String namespaceOpt = "";
    @Parameter(names = {"-nid", "--namespace-id"},
        description = "namespace id to display/set/delete properties for")
    public String namespaceIdOpt = "";
    @Parameter(names = {"-s", "--set"}, description = "set a property")
    public String setOpt = "";
    @Parameter(names = {"-t", "--table"},
        description = "table to display/set/delete properties for")
    public String tableOpt = "";
    @Parameter(names = {"-tid", "--table-id"},
        description = "table id to display/set/delete properties for")
    public String tableIdOpt = "";

    @Override
    public void parseArgs(String programName, String[] args, Object... others) {
      super.parseArgs(programName, args, others);
      var cmdMode = getCmdMode();
      if (cmdMode == Opts.CmdMode.ERROR) {
        throw new IllegalArgumentException("Cannot use set and delete in one command");
      }
    }

    CmdMode getCmdMode() {
      if (!deleteOpt.isEmpty() && !setOpt.isEmpty()) {
        return CmdMode.ERROR;
      }
      if (!deleteOpt.isEmpty()) {
        return CmdMode.DELETE;
      }
      if (!setOpt.isEmpty()) {
        return CmdMode.SET;
      }
      return CmdMode.PRINT;
    }

    enum CmdMode {
      ERROR, PRINT, SET, DELETE
    }
  }

  private static class NullWatcher extends PropStoreWatcher {
    public NullWatcher(ReadyMonitor zkReadyMonitor) {
      super(zkReadyMonitor);
    }
  }
}
