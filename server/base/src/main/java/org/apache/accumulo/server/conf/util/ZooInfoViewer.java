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
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.apache.accumulo.server.conf.util.ZooPropUtils.getNamespaceIdToNameMap;
import static org.apache.accumulo.server.conf.util.ZooPropUtils.getTableIdToName;
import static org.apache.accumulo.server.conf.util.ZooPropUtils.readInstancesFromZk;
import static org.apache.accumulo.server.zookeeper.ZooAclUtil.checkWritableAuth;
import static org.apache.accumulo.server.zookeeper.ZooAclUtil.extractAuthName;
import static org.apache.accumulo.server.zookeeper.ZooAclUtil.translateZooPerm;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerInfo.ServerType;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ReadyMonitor;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.zookeeper.ZooAclUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
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

    var conf = opts.getSiteConfiguration();

    ZooReader zooReader = new ZooReaderWriter(conf);

    try (ServerContext context = new ServerContext(ServerType.UTILITY, conf)) {
      InstanceId iid = context.getInstanceID();
      generateReport(iid, opts, zooReader);
    }
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

      if (opts.printAcls) {
        printAcls(iid, opts, writer);
      }
      writer.println("-----------------------------------------------");
    }
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

  private void printAcls(final InstanceId iid, final Opts opts, final PrintWriter writer) {

    Map<String,List<ACL>> aclMap = new TreeMap<>();

    writer.printf("Output format:\n");
    writer.printf("ACCUMULO_PERM:OTHER_PERM path user_acls...\n\n");

    writer.printf("ZooKeeper acls for instance ID: %s\n\n", iid.canonical());

    ZooKeeper zooKeeper = new ZooReaderWriter(opts.getSiteConfiguration()).getZooKeeper();

    String instanceRoot = ZooUtil.getRoot(iid);

    final Stat stat = new Stat();

    recursiveAclRead(zooKeeper, ZROOT + ZINSTANCES, stat, aclMap);

    recursiveAclRead(zooKeeper, instanceRoot, stat, aclMap);

    // print formatting
    aclMap.forEach((path, acl) -> {
      if (acl == null) {
        writer.printf("ERROR_ACCUMULO_MISSING_SOME: '%s' : none\n", path);
      } else {
        // sort for consistent presentation
        acl.sort(Comparator.comparing(a -> a.getId().getId()));
        ZooAclUtil.ZkAccumuloAclStatus aclStatus = checkWritableAuth(acl);

        String authStatus;
        if (aclStatus.accumuloHasFull()) {
          authStatus = "ACCUMULO_OKAY";
        } else {
          authStatus = "ERROR_ACCUMULO_MISSING_SOME";
        }

        String otherUpdate;
        if (aclStatus.othersMayUpdate() || aclStatus.anyCanRead()) {
          otherUpdate = "NOT_PRIVATE";
        } else {
          otherUpdate = "PRIVATE";
        }

        writer.printf("%s:%s %s", authStatus, otherUpdate, path);
        boolean addSeparator = false;
        for (ACL a : acl) {
          if (addSeparator) {
            writer.printf(",");
          }
          writer.printf(" %s:%s", translateZooPerm(a.getPerms()), extractAuthName(a));
          addSeparator = true;
        }
      }
      writer.println("");
    });
    writer.flush();
  }

  private void recursiveAclRead(final ZooKeeper zooKeeper, final String rootPath, final Stat stat,
      final Map<String,List<ACL>> aclMap) {
    try {
      ZKUtil.visitSubTreeDFS(zooKeeper, rootPath, false, (rc, path, ctx, name) -> {
        try {
          final List<ACL> acls = zooKeeper.getACL(path, stat);
          // List<ACL> acl = getAcls(childPath, dummy);
          aclMap.put(path, acls);

        } catch (KeeperException.NoNodeException ex) {
          throw new IllegalStateException("Node removed during processing", ex);
        } catch (KeeperException ex) {
          throw new IllegalStateException("ZooKeeper exception during processing", ex);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("interrupted during processing", ex);
        }
      });
    } catch (KeeperException ex) {
      throw new IllegalStateException("ZooKeeper exception during processing", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted during processing", ex);
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

    @Parameter(names = {"--print-acls"},
        description = "print the current acls for all ZooKeeper nodes. The acls are evaluated in context of Accumulo "
            + "operations. Context: ACCUMULO_OKAY | ERROR_ACCUMULO_MISSING_SOME - Accumulo requires cdwra for ZooKeeper "
            + " nodes that it uses. PRIVATE | NOT_PRIVATE - other than configuration, most nodes are world read-able "
            + "(NOT_PRIVATE) to permit client access")
    public boolean printAcls = false;

    @Parameter(names = {"--print-id-map"},
        description = "print the namespace and table id, name mappings stored in ZooKeeper")
    public boolean printIdMap = false;

    @Parameter(names = {"--print-props"},
        description = "print the property values stored in ZooKeeper, can be filtered with --system, --namespaces and --tables options")
    public boolean printProps = false;

    @Parameter(names = {"--print-instances"},
        description = "print the instance ids stored in ZooKeeper")
    public boolean printInstanceIds = false;

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
