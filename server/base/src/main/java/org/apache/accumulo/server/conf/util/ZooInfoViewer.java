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
import static org.apache.accumulo.core.Constants.ZINSTANCES;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZNAMESPACE_NAME;
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_NAME;
import static org.apache.accumulo.core.Constants.ZTABLE_NAMESPACE;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
@SuppressFBWarnings(value = "PATH_TRAVERSAL_OUT",
    justification = "app is run in same security context as user providing the filename")
public class ZooInfoViewer implements KeywordExecutable {
  private static final DateTimeFormatter tsFormat =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));
  private static final Logger log = LoggerFactory.getLogger(ZooInfoViewer.class);
  private final NullWatcher nullWatcher =
      new NullWatcher(new ReadyMonitor(ZooInfoViewer.class.getSimpleName(), 20_000L));

  private static final String INDENT = "  ";

  /**
   * No-op constructor - provided so ServiceLoader autoload does not consume resources.
   */
  public ZooInfoViewer() {}

  public static void main(String[] args) throws Exception {
    new ZooInfoViewer().execute(args);
  }

  @Override
  public String keyword() {
    return "zoo-info-viewer";
  }

  @Override
  public String description() {
    return "view Accumulo instance and property information stored in ZooKeeper";
  }

  @Override
  public void execute(String[] args) throws Exception {

    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(), args);

    log.info("print ids map: {}", opts.printIdMap);
    log.info("print properties: {}", opts.printProps);
    log.info("print instances: {}", opts.printInstanceIds);

    ZooReader zooReader = new ZooReaderWriter(opts.getSiteConfiguration());

    InstanceId iid = getInstanceId(zooReader, opts);
    generateReport(iid, opts, zooReader);
  }

  void generateReport(final InstanceId iid, final ZooInfoViewer.Opts opts,
      final ZooReader zooReader) throws Exception {

    OutputStream outStream;

    String outfile = opts.getOutfile();
    if (outfile == null || outfile.isEmpty()) {
      log.trace("No output file, using stdout.");
      outStream = System.out;
    } else {
      outStream = new FileOutputStream(outfile);
    }

    try (PrintWriter writer =
        new PrintWriter(new BufferedWriter(new OutputStreamWriter(outStream, UTF_8)))) {

      writer.println("-----------------------------------------------");
      writer.println("Report Time: " + tsFormat.format(Instant.now()));
      writer.println("-----------------------------------------------");
      if (opts.printInstanceIds) {
        Map<String,InstanceId> instanceMap = readInstancesFromZk(zooReader);
        printInstanceIds(instanceMap, writer);
      }

      if (opts.printIdMap) {
        printIdMapping(iid, zooReader, writer);
      }

      if (opts.printProps) {
        printProps(iid, zooReader, opts, writer);
      }

      writer.println("-----------------------------------------------");
    }
  }

  /**
   * Get the instanceID from the command line options, or from value stored in HDFS. The search
   * order is:
   * <ol>
   * <li>command line: --instanceId option</li>
   * <li>command line: --instanceName option</li>
   * <li>HDFS</li>
   * </ol>
   *
   * @param zooReader a ZooReader
   * @param opts the parsed command line options.
   * @return an instance id
   */
  InstanceId getInstanceId(final ZooReader zooReader, final ZooInfoViewer.Opts opts) {

    if (!opts.instanceId.isEmpty()) {
      return InstanceId.of(opts.instanceId);
    }
    if (!opts.instanceName.isEmpty()) {
      Map<String,InstanceId> instanceNameToIdMap = readInstancesFromZk(zooReader);
      String instanceName = opts.instanceName;
      for (Map.Entry<String,InstanceId> e : instanceNameToIdMap.entrySet()) {
        if (e.getKey().equals(instanceName)) {
          return e.getValue();
        }
      }
      throw new IllegalArgumentException(
          "Specified instance name '" + instanceName + "' not found in ZooKeeper");
    }

    try (ServerContext context = new ServerContext(SiteConfiguration.auto())) {
      return context.getInstanceID();
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          "Failed to read instance id from HDFS. Instances can be specified on the command line",
          ex);
    }
  }

  Map<NamespaceId,String> getNamespaceIdToNameMap(InstanceId iid, final ZooReader zooReader) {
    SortedMap<NamespaceId,String> namespaceToName = new TreeMap<>();
    String zooNsRoot = ZooUtil.getRoot(iid) + ZNAMESPACES;
    try {
      List<String> nsids = zooReader.getChildren(zooNsRoot);
      for (String id : nsids) {
        String path = zooNsRoot + "/" + id + ZNAMESPACE_NAME;
        String name = new String(zooReader.getData(path), UTF_8);
        namespaceToName.put(NamespaceId.of(id), name);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading namespace ids from ZooKeeper", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to read namespace ids from ZooKeeper", ex);
    }
    return namespaceToName;
  }

  private void printProps(final InstanceId iid, final ZooReader zooReader, final Opts opts,
      final PrintWriter writer) throws Exception {

    if (opts.printAllProps()) {
      log.info("all: {}", opts.printAllProps());
    } else {
      log.info("Filters:");
      log.info("system: {}", opts.printSysProps());
      log.info("namespaces: {} {}", opts.printNamespaceProps(),
          opts.getNamespaces().size() > 0 ? opts.getNamespaces() : "");
      log.info("tables: {} {}", opts.printTableProps(),
          opts.getTables().size() > 0 ? opts.getTables() : "");
    }

    writer.printf("ZooKeeper properties for instance ID: %s\n\n", iid.canonical());
    if (opts.printSysProps()) {
      printSortedProps(writer, Map.of("System", fetchSystemProp(iid, zooReader)));
    }

    if (opts.printNamespaceProps()) {
      Map<NamespaceId,String> id2NamespaceMap = getNamespaceIdToNameMap(iid, zooReader);

      Map<String,VersionedProperties> nsProps =
          fetchNamespaceProps(iid, zooReader, id2NamespaceMap, opts.getNamespaces());

      writer.println("Namespace: ");
      printSortedProps(writer, nsProps);
      writer.flush();
    }

    if (opts.printTableProps()) {
      Map<String,VersionedProperties> tProps = fetchTableProps(iid, opts.getTables(), zooReader);
      writer.println("Tables: ");
      printSortedProps(writer, tProps);
    }
    writer.println();
  }

  private void printIdMapping(InstanceId iid, ZooReader zooReader, PrintWriter writer) {
    // namespaces
    Map<NamespaceId,String> id2NamespaceMap = getNamespaceIdToNameMap(iid, zooReader);
    writer.println("ID Mapping (id => name) for instance: " + iid);
    writer.println("Namespace ids:");
    for (Map.Entry<NamespaceId,String> e : id2NamespaceMap.entrySet()) {
      String v = e.getValue().isEmpty() ? "\"\"" : e.getValue();
      writer.printf("%s%-9s => %24s\n", INDENT, e.getKey(), v);
    }
    writer.println();
    // tables
    Map<TableId,String> id2TableMap = getTableIdToName(iid, id2NamespaceMap, zooReader);
    writer.println("Table ids:");
    for (Map.Entry<TableId,String> e : id2TableMap.entrySet()) {
      writer.printf("%s%-9s => %24s\n", INDENT, e.getKey(), e.getValue());
    }
    writer.println();
  }

  /**
   * Read the instance names and instance ids from ZooKeeper. The storage structure in ZooKeeper is:
   *
   * <pre>
   *   /accumulo/instances/instance_name  - with the instance id stored as data.
   * </pre>
   *
   * @return a map of (instance name, instance id) entries
   */
  Map<String,InstanceId> readInstancesFromZk(final ZooReader zooReader) {
    String instanceRoot = ZROOT + ZINSTANCES;
    Map<String,InstanceId> idMap = new TreeMap<>();
    try {
      List<String> names = zooReader.getChildren(instanceRoot);
      names.forEach(name -> {
        InstanceId iid = getInstanceIdForName(zooReader, name);
        idMap.put(name, iid);
      });
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance name info from ZooKeeper", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to read instance name info from ZooKeeper", ex);
    }
    return idMap;
  }

  private InstanceId getInstanceIdForName(ZooReader zooReader, String name) {
    String instanceRoot = ZROOT + ZINSTANCES;
    String path = "";
    try {
      path = instanceRoot + "/" + name;
      byte[] uuid = zooReader.getData(path);
      return InstanceId.of(UUID.fromString(new String(uuid, UTF_8)));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance id from ZooKeeper", ex);
    } catch (KeeperException ex) {
      log.warn("Failed to read instance id for " + path);
      return null;
    }
  }

  private void printInstanceIds(final Map<String,InstanceId> instanceIdMap, PrintWriter writer) {
    writer.println("Instances (Instance Name, Instance ID)");
    instanceIdMap.forEach((name, iid) -> writer.println(name + "=" + iid));
    writer.println();
  }

  private Map<String,VersionedProperties> fetchNamespaceProps(InstanceId iid, ZooReader zooReader,
      Map<NamespaceId,String> id2NamespaceMap, List<String> namespaces) {

    Set<String> cmdOptNamespaces = new TreeSet<>(namespaces);

    Map<NamespaceId,String> filteredIds;
    if (cmdOptNamespaces.isEmpty()) {
      filteredIds = id2NamespaceMap;
    } else {
      filteredIds =
          id2NamespaceMap.entrySet().stream().filter(e -> cmdOptNamespaces.contains(e.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    log.trace("ns filter: {}", filteredIds);
    Map<String,VersionedProperties> results = new TreeMap<>();

    filteredIds.forEach((nid, name) -> {
      try {
        var key = NamespacePropKey.of(iid, nid);
        log.trace("fetch props from path: {}", key.getPath());
        var props = ZooPropStore.readFromZk(key, nullWatcher, zooReader);
        results.put(name, props);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted reading table properties from ZooKeeper", ex);
      } catch (IOException | KeeperException ex) {
        throw new IllegalStateException("Failed to read table properties from ZooKeeper", ex);
      }
    });

    return results;
  }

  private Map<String,VersionedProperties> fetchTableProps(final InstanceId iid,
      final List<String> tables, final ZooReader zooReader) {

    Set<String> cmdOptTables = new TreeSet<>(tables);

    Map<NamespaceId,String> id2NamespaceMap = getNamespaceIdToNameMap(iid, zooReader);
    Map<TableId,String> allIds = getTableIdToName(iid, id2NamespaceMap, zooReader);

    Map<TableId,String> filteredIds;
    if (cmdOptTables.isEmpty()) {
      filteredIds = allIds;
    } else {
      filteredIds = allIds.entrySet().stream().filter(e -> cmdOptTables.contains(e.getValue()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    log.trace("Looking for: {}", filteredIds);

    Map<String,VersionedProperties> results = new TreeMap<>();

    filteredIds.forEach((tid, name) -> {
      try {
        var key = TablePropKey.of(iid, tid);
        log.trace("fetch props from path: {}", key.getPath());
        var props = ZooPropStore.readFromZk(key, nullWatcher, zooReader);
        results.put(name, props);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted reading table properties from ZooKeeper", ex);
      } catch (IOException | KeeperException ex) {
        throw new IllegalStateException("Failed to read table properties from ZooKeeper", ex);
      }
    });

    return results;
  }

  private Map<TableId,String> getTableIdToName(InstanceId iid,
      Map<NamespaceId,String> id2NamespaceMap, ZooReader zooReader) {
    SortedMap<TableId,String> idToName = new TreeMap<>();

    String zooTables = ZooUtil.getRoot(iid) + ZTABLES;
    try {
      List<String> tids = zooReader.getChildren(zooTables);
      for (String t : tids) {
        String path = zooTables + "/" + t;
        String tname = new String(zooReader.getData(path + ZTABLE_NAME), UTF_8);
        NamespaceId tNsId =
            NamespaceId.of(new String(zooReader.getData(path + ZTABLE_NAMESPACE), UTF_8));
        if (tNsId.equals(Namespace.DEFAULT.id())) {
          idToName.put(TableId.of(t), tname);
        } else {
          idToName.put(TableId.of(t), id2NamespaceMap.get(tNsId) + "." + tname);
        }
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading table ids from ZooKeeper", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed reading table id info from ZooKeeper");
    }
    return idToName;
  }

  private void printSortedProps(final PrintWriter writer,
      final Map<String,VersionedProperties> props) {
    log.trace("Printing: {}", props);
    props.forEach((n, p) -> {
      if (p == null) {
        writer.printf("Name: '%s' : no property node present\n", n);
      } else {
        writer.printf("Name: %s, Data Version:%s, Data Timestamp: %s:\n", n, p.getDataVersion(),
            tsFormat.format(p.getTimestamp()));
        Map<String,String> pMap = p.asMap();
        if (pMap.isEmpty()) {
          writer.println("-- none --");
        } else {
          TreeMap<String,String> sorted = new TreeMap<>(pMap);
          sorted.forEach((name, value) -> writer.printf("%s%s=%s\n", INDENT, name, value));
        }
        writer.println();
      }
    });
  }

  private VersionedProperties fetchSystemProp(final InstanceId iid, final ZooReader zooReader)
      throws Exception {
    SystemPropKey propKey = SystemPropKey.of(iid);
    return ZooPropStore.readFromZk(propKey, nullWatcher, zooReader);
  }

  static class Opts extends ConfigOpts {
    @Parameter(names = {"--outfile"},
        description = "Write the output to a file, if the file exists will not be overwritten.")
    public String outfile = "";

    @Parameter(names = {"--print-id-map"},
        description = "print the namespace and table id, name mappings stored in ZooKeeper")
    public boolean printIdMap = false;

    @Parameter(names = {"--print-props"},
        description = "print the property values stored in ZooKeeper, can be filtered with --system, --namespaces and --tables options")
    public boolean printProps = false;

    @Parameter(names = {"--print-instances"},
        description = "print the instance ids stored in ZooKeeper")
    public boolean printInstanceIds = false;

    @Parameter(names = {"--instanceName"},
        description = "Specify the instance name to use. If instance name or id are not provided, determined from configuration (requires a running hdfs instance)")
    public String instanceName = "";

    @Parameter(names = {"--instanceId"},
        description = "Specify the instance id to use. If instance name or id are not provided, determined from configuration (requires a running hdfs instance)")
    public String instanceId = "";

    @Parameter(names = {"-ns", "--namespaces"},
        description = "a list of namespace names to print properties, with none specified, print all. Only valid with --print-props",
        variableArity = true)
    private List<String> namespacesOpt = new ArrayList<>();

    @Parameter(names = {"--system"},
        description = "print the properties for the system config. Only valid with --print-props")
    private boolean printSystemOpt = false;

    @Parameter(names = {"-t", "--tables"},
        description = "a list of table names to print properties, with none specified, print all. Only valid with --print-props",
        variableArity = true)
    private List<String> tablesOpt = new ArrayList<>();

    /**
     * Get print all option status.
     *
     * @return true if print all is set AND no namespaces or table names were provided.
     */
    boolean printAllProps() {
      return !printSystemOpt && namespacesOpt.isEmpty() && tablesOpt.isEmpty();
    }

    boolean printSysProps() {
      return printAllProps() || printSystemOpt;
    }

    boolean printNamespaceProps() {
      return printAllProps() || !namespacesOpt.isEmpty();
    }

    List<String> getNamespaces() {
      return namespacesOpt;
    }

    boolean printTableProps() {
      return printAllProps() || !tablesOpt.isEmpty();
    }

    List<String> getTables() {
      return tablesOpt;
    }

    String getOutfile() {
      return outfile;
    }
  }

  private static class NullWatcher extends PropStoreWatcher {
    public NullWatcher(ReadyMonitor zkReadyMonitor) {
      super(zkReadyMonitor);
    }
  }
}
