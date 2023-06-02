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
import static org.apache.accumulo.server.conf.util.ZooPropUtils.getInstanceId;
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
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
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
public class ZooPropSetTool implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropSetTool.class);
  private final NullWatcher nullWatcher =
      new NullWatcher(new ReadyMonitor(ZooInfoViewer.class.getSimpleName(), 20_000L));

  /**
   * No-op constructor - provided so ServiceLoader autoload does not consume resources.
   */
  public ZooPropSetTool() {}

  public static void main(String[] args) throws Exception {
    new ZooPropSetTool().execute(args);
  }

  @Override
  public String keyword() {
    return "zoo-prop-set-tool";
  }

  @Override
  public String description() {
    return "Emergency tool to modify properties stored in ZooKeeper without a cluster."
        + " Prefer using the shell if it is available";
  }

  @Override
  public void execute(String[] args) throws Exception {
    ZooPropSetTool.Opts opts = new ZooPropSetTool.Opts();
    opts.parseArgs(ZooPropSetTool.class.getName(), args);

    ZooReaderWriter zrw = new ZooReaderWriter(opts.getSiteConfiguration());

    InstanceId iid = getInstanceId(zrw, opts.instanceId, opts.instanceName);
    if (iid == null) {
      throw new IllegalArgumentException("Cannot continue without a valid instance.");
    }

    var siteConfig = opts.getSiteConfiguration();
    try (ServerContext context = new ServerContext(siteConfig)) {

      PropStoreKey<?> propKey = getPropKey(iid, opts, zrw);
      switch (opts.getCmdMode()) {
        case SET:
          setProperty(context, propKey, opts);
          break;
        case DELETE:
          deleteProperty(context, propKey, readPropNode(propKey, zrw), opts);
          break;
        case PRINT:
          printProperties(context, propKey, readPropNode(propKey, zrw), opts);
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
    String[] tokens = opts.setOpt.split("=");
    Map<String,String> propValue = Map.of(tokens[0].trim(), tokens[1].trim());
    PropUtil.setProperties(context, propKey, propValue);
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
  }

  private void printProperties(final ServerContext context, final PropStoreKey<?> propKey,
      final VersionedProperties props, final Opts opts) {
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
      writer.printf("- Instance name: %s\n", context.getInstanceName());
      writer.printf("- Instance id: %s\n", context.getInstanceID());
      writer.printf("- Property scope: - %s\n", scope);
      writer.printf("- id: %s, data version: %d, timestamp: %s\n", propKey.getId(),
          props.getDataVersion(), props.getTimestampISO());

      // skip filtering if no props
      if (props.asMap().isEmpty()) {
        writer.println("none");
        return;
      }

      SortedMap<String,String> sortedMap = filterProps(props, opts);
      // skip print if all filtered out
      if (sortedMap.isEmpty()) {
        writer.println("none");
        return;
      }
      sortedMap.forEach((name, value) -> writer.printf("%s=%s\n", name, value));
    }
  }

  /**
   * If a filter is provided, filter the properties and return the map sorted for consistent
   * presentation. If no filter is provided, all properties for the property node are returned.
   */
  private SortedMap<String,String> filterProps(VersionedProperties props, Opts opts) {
    var propsMap = props.asMap();

    Filter filter = opts.getFilter();
    switch (filter.getMode()) {
      case NAME:
        String nameFilter = filter.getString();
        return new TreeMap<>(
            propsMap.entrySet().stream().filter(e -> e.getKey().contains(nameFilter))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      case NAME_VAL:
        String nvFilter = filter.getString();
        return new TreeMap<>(propsMap.entrySet().stream()
            .filter(e -> (e.getKey().contains(nvFilter) || e.getValue().contains(nvFilter)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      default:
        return new TreeMap<>(propsMap);
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

  private PropStoreKey<?> getPropKey(final InstanceId iid, final ZooPropSetTool.Opts opts,
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

  private TableId getTableId(final InstanceId iid, final ZooPropSetTool.Opts opts,
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

  private NamespaceId getNamespaceId(final InstanceId iid, final ZooPropSetTool.Opts opts,
      final ZooReader zooReader) {
    if (!opts.namespaceIdOpt.isEmpty()) {
      return NamespaceId.of(opts.namespaceIdOpt);
    }
    Map<NamespaceId,String> nids = getNamespaceIdToNameMap(iid, zooReader);
    return nids.entrySet().stream().filter(entry -> opts.namespaceOpt.equals(entry.getValue()))
        .map(Map.Entry::getKey).findAny().orElseThrow(
            () -> new IllegalArgumentException("Could not find namespace " + opts.namespaceOpt));
  }

  static class Opts extends ConfigOpts {

    @Parameter(names = {"-d", "--delete"}, description = "delete a property")
    public String deleteOpt = "";
    @Parameter(names = {"-f", "--filter"},
        description = "show only properties that contain this string in their name.")
    public String filterOpt = "";
    @Parameter(names = {"-fv", "--filter-with-values"},
        description = "show only properties that contain this string in their name.")
    public String filterWithValuesOpt = "";
    @Parameter(names = {"--instanceName"},
        description = "Specify the instance name to use. If instance name or id are not provided, determined from configuration (requires a running hdfs instance)")
    public String instanceName = "";
    @Parameter(names = {"--instanceId"},
        description = "Specify the instance id to use. If instance name or id are not provided, determined from configuration (requires a running hdfs instance)")
    public String instanceId = "";
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

    private Filter filter = null;

    @Override
    public void parseArgs(String programName, String[] args, Object... others) {
      super.parseArgs(programName, args, others);
      var cmdMode = getCmdMode();
      if (cmdMode == Opts.CmdMode.ERROR) {
        throw new IllegalArgumentException("Cannot use set and delete in one command");
      }
      filter = new Filter(this);
      if (!filter.getString().isEmpty()
          && (cmdMode == Opts.CmdMode.SET || cmdMode == CmdMode.DELETE)) {
        throw new IllegalArgumentException("Cannot use filter with set or delete");
      }
    }

    Filter getFilter() {
      return filter;
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

  /**
   * The filter type and the filter string specified by command line options. Filter types are - no
   * filter, filter on name or filter by name or value.
   */
  private static class Filter {
    private final ZooPropSetTool.Opts opts;

    enum Mode {
      NONE, NAME, NAME_VAL
    }

    public Filter(ZooPropSetTool.Opts opts) {
      this.opts = opts;
    }

    Mode getMode() {
      if (!opts.filterOpt.isEmpty() && !opts.filterWithValuesOpt.isEmpty()) {
        LOG.info(
            "Filter by name and by name and value provided, using by name and value with: `{}`",
            opts.filterWithValuesOpt);
        return Mode.NAME_VAL;
      }

      if (!opts.filterOpt.isEmpty()) {
        LOG.trace("Filter by name with: `{}`", opts.filterOpt);
        return Mode.NAME;
      }

      if (!opts.filterWithValuesOpt.isEmpty()) {
        LOG.trace("Filter by name and values with: `{}`", opts.filterWithValuesOpt);
        return Mode.NAME_VAL;
      }
      return Mode.NONE;
    }

    String getString() {
      Filter.Mode mode = getMode();
      switch (mode) {
        case NAME:
          return opts.filterOpt;
        case NAME_VAL:
          return opts.filterWithValuesOpt;
        default:
          return "";
      }
    }
  }

  private static class NullWatcher extends PropStoreWatcher {
    public NullWatcher(ReadyMonitor zkReadyMonitor) {
      super(zkReadyMonitor);
    }
  }
}
