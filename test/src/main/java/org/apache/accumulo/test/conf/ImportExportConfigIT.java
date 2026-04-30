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
package org.apache.accumulo.test.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.util.ExportConfigCommand;
import org.apache.accumulo.server.conf.util.ImportConfigCommand;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.constructor.DuplicateKeyException;

public class ImportExportConfigIT extends AccumuloClusterHarness {

  @TempDir
  private static Path tempDir;

  private static final String YAML1 =
      """
          scope: SYSTEM
          name: ''
          properties:
            compaction.service.maintenance.planner: org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner
            compaction.service.maintenance.planner.opts.groups: |2
               [{"group":"prod_maintenance_compactors"}]
            compaction.service.prod.planner: org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner
            compaction.service.prod.planner.opts.groups: |2
               [{"group":"prod_small_compactors", "maxSize":"256M"},{"group":"prod_large_compactors"}]
            compaction.service.test.planner: org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner
            compaction.service.test.planner.opts.groups: |2
               [{"group":"test_small_compactors", "maxSize":"256M"},{"group":"test_large_compactors"}]
          ---
          scope: RESOURCE_GROUP
          name: default
          properties: {
            }
          ---
          scope: RESOURCE_GROUP
          name: prod
          properties:
            general.special.chars: "!@#$%^&*()_\\n+-=<,>.?/\\t:;\\"''{[}]\\\\~`|"
            sserver.cache.data.size: 16G
            sserver.cache.index.size: 2G
            tserver.cache.data.size: 32G
            tserver.cache.index.size: 4G
          ---
          scope: RESOURCE_GROUP
          name: prod_large_compactors
          properties: {
            compactor.failure.termination.threshold: 5,
            compactor.wait.time.job.max: 3m
          }
          ---
          scope: RESOURCE_GROUP
          name: prod_maintenance_compactors
          properties: {
            }
          ---
          scope: RESOURCE_GROUP
          name: prod_small_compactors
          properties: {
            compactor.failure.termination.threshold: 10
          }
          ---
          scope: RESOURCE_GROUP
          name: test
          properties: {
            tserver.cache.data.size: 2G,
            tserver.cache.index.size: 512M
          }
          ---
          scope: RESOURCE_GROUP
          name: test_large_compactors
          properties: {
            compactor.wait.time.job.max: 20s,
            compactor.wait.time.job.min: 10ms
          }
          ---
          scope: RESOURCE_GROUP
          name: test_small_compactors
          properties: {
            compactor.wait.time.job.max: 10s,
            compactor.wait.time.job.min: 10ms
          }
          ---
          scope: NAMESPACE
          name: ''
          properties: {
            }
          ---
          scope: NAMESPACE
          name: accumulo
          properties: {
            }
          ---
          scope: NAMESPACE
          name: production
          properties: {
            table.compaction.dispatcher.opts.service: prod,
            table.compaction.dispatcher.opts.service.user: maintenance,
            table.custom.assignment.group: prod
          }
          ---
          scope: NAMESPACE
          name: testing
          properties: {
            table.compaction.dispatcher.opts.service: test,
            table.custom.assignment.group: test
          }
          ---
          scope: TABLE
          name: accumulo.fate
          properties: {
            table.cache.block.enable: true,
            table.cache.index.enable: true,
            table.compaction.major.ratio: 1,
            table.durability: sync,
            table.failures.ignore: false,
            table.file.compress.blocksize: 32K,
            table.file.replication: 5,
            table.group.txAdmin: txadmin,
            table.groups.enabled: txAdmin,
            table.iterator.majc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1,
            table.security.scan.visibility.default: '',
            table.split.threshold: 256M
          }
          ---
          scope: TABLE
          name: accumulo.metadata
          properties: {
            table.cache.block.enable: true,
            table.cache.index.enable: true,
            table.compaction.major.ratio: 1,
            table.constraint.1: org.apache.accumulo.server.constraints.MetadataConstraints,
            table.durability: sync,
            table.failures.ignore: false,
            table.file.compress.blocksize: 32K,
            table.file.replication: 5,
            table.group.server: 'file,log,srv,future',
            table.group.tablet: '~tab,loc',
            table.groups.enabled: 'tablet,server',
            table.iterator.majc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1,
            table.security.scan.visibility.default: '',
            table.split.threshold: 1234K
          }
          ---
          scope: TABLE
          name: accumulo.root
          properties: {
            table.cache.block.enable: true,
            table.cache.index.enable: true,
            table.compaction.major.ratio: 1,
            table.constraint.1: org.apache.accumulo.server.constraints.MetadataConstraints,
            table.durability: sync,
            table.failures.ignore: false,
            table.file.compress.blocksize: 32K,
            table.file.replication: 5,
            table.group.server: 'file,log,srv,future',
            table.group.tablet: '~tab,loc',
            table.groups.enabled: 'tablet,server',
            table.iterator.majc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1,
            table.security.scan.visibility.default: '',
            table.split.threshold: 64M
          }
          ---
          scope: TABLE
          name: accumulo.scanref
          properties: {
            table.cache.block.enable: true,
            table.cache.index.enable: true,
            table.compaction.major.ratio: 1,
            table.durability: sync,
            table.failures.ignore: false,
            table.file.compress.blocksize: 32K,
            table.file.replication: 5,
            table.iterator.majc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.vers: '10,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1,
            table.security.scan.visibility.default: ''
          }
          ---
          scope: TABLE
          name: production.customers
          properties: {
            table.constraint.1: org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint,
            table.file.max: 7,
            table.iterator.majc.filter: '200,WorkingFilter.class',
            table.iterator.majc.filter.opt.dropMissing: false,
            table.iterator.majc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.filter: '200,WorkingFilter.class',
            table.iterator.minc.filter.opt.dropMissing: false,
            table.iterator.minc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.filter: '200,WorkingFilter.class',
            table.iterator.scan.filter.opt.dropMissing: false,
            table.iterator.scan.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1
          }
          ---
          scope: TABLE
          name: production.payments
          properties: {
            table.cache.block.enable: true,
            table.constraint.1: org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint,
            table.file.compress.blocksize.index: 32K,
            table.iterator.majc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 1,
            table.iterator.minc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 1,
            table.iterator.scan.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 1
          }
          ---
          scope: TABLE
          name: testing.customers
          properties: {
            table.iterator.majc.betaFilter: '200,ExperimentalFilter.class',
            table.iterator.majc.betaFilter.opt.dropMissing: true,
            table.iterator.minc.betaFilter: '200,ExperimentalFilter.class',
            table.iterator.minc.betaFilter.opt.dropMissing: true,
            table.iterator.scan.betaFilter: '200,ExperimentalFilter.class',
            table.iterator.scan.betaFilter.opt.dropMissing: true
          }
          ---
          scope: TABLE
          name: testing.payments
          properties: {
            table.iterator.majc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.majc.vers.opt.maxVersions: 10,
            table.iterator.minc.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.minc.vers.opt.maxVersions: 10,
            table.iterator.scan.vers: '20,org.apache.accumulo.core.iterators.user.VersioningIterator',
            table.iterator.scan.vers.opt.maxVersions: 10
          }
          """;

  private final static Map<String,String> SYSTEM_PROPS;
  private final static Map<ResourceGroupId,Map<String,String>> RG_PROPS;
  private final static Map<String,Map<String,String>> NAMESPACE_PROPS;

  record TableChanges(String name, boolean create, boolean noDefaults, List<IteratorSetting> iters,
      Map<String,String> props, Set<String> delete) {
  }

  private final static List<TableChanges> TABLE_CHANGES;

  private final static ImportConfigCommand.Opts DEFAULT_OPTS = new ImportConfigCommand.Opts();
  private final static ImportConfigCommand.Opts IGNORE_EXTRA_OPTS = new ImportConfigCommand.Opts();
  private final static ImportConfigCommand.Opts DRY_RUN_IGNORE_EXTRA_OPTS =
      new ImportConfigCommand.Opts();
  private final static ImportConfigCommand.Opts DRY_RUN_OPTS = new ImportConfigCommand.Opts();
  static {
    var systemProps = new HashMap<String,String>();

    var testCompactionService = """
         [{"group":"test_small_compactors", "maxSize":"256M"},{"group":"test_large_compactors"}]
        """;
    var prodCompactionService = """
         [{"group":"prod_small_compactors", "maxSize":"256M"},{"group":"prod_large_compactors"}]
        """;
    var prodMaintenanceService = """
         [{"group":"prod_maintenance_compactors"}]
        """;
    systemProps.put("compaction.service.prod.planner",
        "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner");
    systemProps.put("compaction.service.prod.planner.opts.groups", prodCompactionService);
    systemProps.put("compaction.service.maintenance.planner",
        "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner");
    systemProps.put("compaction.service.maintenance.planner.opts.groups", prodMaintenanceService);
    systemProps.put("compaction.service.test.planner",
        "org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner");
    systemProps.put("compaction.service.test.planner.opts.groups", testCompactionService);

    SYSTEM_PROPS = Map.copyOf(systemProps);

    var rgProps = new HashMap<ResourceGroupId,Map<String,String>>();
    var prodProps = new HashMap<String,String>();
    prodProps.put(Property.TSERV_DATACACHE_SIZE.getKey(), "32G");
    prodProps.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "4G");
    prodProps.put(Property.SSERV_DATACACHE_SIZE.getKey(), "16G");
    prodProps.put(Property.SSERV_INDEXCACHE_SIZE.getKey(), "2G");
    // Try setting special chars and ensure they make it through encoding to/from yaml
    prodProps.put(Property.GENERAL_PREFIX + "special.chars",
        "!@#$%^&*()_\n+-=<,>.?/\t:;\"''{[}]\\~`|");

    rgProps.put(ResourceGroupId.of("prod"), Map.copyOf(prodProps));

    var testProps = new HashMap<String,String>();
    testProps.put(Property.TSERV_DATACACHE_SIZE.getKey(), "2G");
    testProps.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "512M");
    rgProps.put(ResourceGroupId.of("test"), Map.copyOf(testProps));

    rgProps.put(ResourceGroupId.of("prod_small_compactors"),
        Map.of(Property.COMPACTOR_FAILURE_TERMINATION_THRESHOLD.getKey(), "10"));
    rgProps.put(ResourceGroupId.of("prod_large_compactors"),
        Map.of(Property.COMPACTOR_FAILURE_TERMINATION_THRESHOLD.getKey(), "5",
            Property.COMPACTOR_MAX_JOB_WAIT_TIME.getKey(), "3m"));
    rgProps.put(ResourceGroupId.of("prod_maintenance_compactors"), Map.of());

    rgProps.put(ResourceGroupId.of("test_small_compactors"),
        Map.of(Property.COMPACTOR_MIN_JOB_WAIT_TIME.getKey(), "10ms",
            Property.COMPACTOR_MAX_JOB_WAIT_TIME.getKey(), "10s"));
    rgProps.put(ResourceGroupId.of("test_large_compactors"),
        Map.of(Property.COMPACTOR_MIN_JOB_WAIT_TIME.getKey(), "10ms",
            Property.COMPACTOR_MAX_JOB_WAIT_TIME.getKey(), "20s"));

    RG_PROPS = Map.copyOf(rgProps);

    var testingNsProps = Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service",
        "test", "table.custom.assignment.group", "test");
    var prodNsProps = Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "prod",
        Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service.user", "maintenance",
        "table.custom.assignment.group", "prod");
    NAMESPACE_PROPS = Map.of("testing", testingNsProps, "production", prodNsProps);

    var tableChanges = new ArrayList<TableChanges>();

    var iterSetting1 = new IteratorSetting(200, "betaFilter", "ExperimentalFilter.class");
    iterSetting1.addOption("dropMissing", "true");
    tableChanges.add(new TableChanges("testing.customers", true, true, List.of(iterSetting1),
        Map.of(), Set.of()));
    // remove and change some of the tables default properties
    tableChanges.add(new TableChanges("testing.payments", true, false, List.of(),
        Map.of("table.iterator.majc.vers.opt.maxVersions", "10",
            "table.iterator.minc.vers.opt.maxVersions", "10",
            "table.iterator.scan.vers.opt.maxVersions", "10"),
        Set.of("table.constraint.1")));
    var iterSetting2 = new IteratorSetting(200, "filter", "WorkingFilter.class");
    iterSetting2.addOption("dropMissing", "false");
    tableChanges.add(new TableChanges("production.customers", true, false, List.of(iterSetting2),
        Map.of(Property.TABLE_FILE_MAX.getKey(), "7"), Set.of()));
    tableChanges.add(new TableChanges("production.payments", true, false, List.of(),
        Map.of(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true",
            Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "32K"),
        Set.of()));
    // override a metadata table property
    tableChanges.add(new TableChanges("accumulo.metadata", false, false, List.of(),
        Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), "1234K"), Set.of()));
    TABLE_CHANGES = List.copyOf(tableChanges);

    IGNORE_EXTRA_OPTS.ignoreExtra = true;
    DRY_RUN_IGNORE_EXTRA_OPTS.dryRun = true;
    DRY_RUN_IGNORE_EXTRA_OPTS.ignoreExtra = true;
    DRY_RUN_OPTS.dryRun = true;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Processes are killed so set this to make WALs works
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testExport() throws Exception {

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      for (var entry : SYSTEM_PROPS.entrySet()) {
        client.instanceOperations().setProperty(entry.getKey(), entry.getValue());
      }

      for (var entry : RG_PROPS.entrySet()) {
        client.resourceGroupOperations().create(entry.getKey());
        for (var pe : entry.getValue().entrySet()) {
          client.resourceGroupOperations().setProperty(entry.getKey(), pe.getKey(), pe.getValue());
        }
      }

      for (var entry : NAMESPACE_PROPS.entrySet()) {
        client.namespaceOperations().create(entry.getKey());
        for (var pe : entry.getValue().entrySet()) {
          client.namespaceOperations().setProperty(entry.getKey(), pe.getKey(), pe.getValue());
        }
      }

      for (var tableProps : TABLE_CHANGES) {
        if (tableProps.create()) {
          if (tableProps.noDefaults()) {
            client.tableOperations().create(tableProps.name(),
                new NewTableConfiguration().withoutDefaults());
          } else {
            client.tableOperations().create(tableProps.name);
          }
        }

        for (var iter : tableProps.iters()) {
          client.tableOperations().attachIterator(tableProps.name(), iter);
        }

        for (var entry : tableProps.props().entrySet()) {
          client.tableOperations().setProperty(tableProps.name(), entry.getKey(), entry.getValue());
        }

        for (var delProp : tableProps.delete()) {
          client.tableOperations().removeProperty(tableProps.name(), delProp);
        }
      }
    }

    var yaml = ExportConfigCommand.export(getServerContext());
    assertEquals(YAML1, yaml);
  }

  @Test
  public void testImport() throws Exception {

    Map<String,Map<String,String>> tablePropsBefore = new HashMap<>();

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      for (var entry : RG_PROPS.entrySet()) {
        client.resourceGroupOperations().create(entry.getKey());
      }

      for (var entry : NAMESPACE_PROPS.entrySet()) {
        client.namespaceOperations().create(entry.getKey());
      }

      for (var tableProps : TABLE_CHANGES) {
        if (tableProps.create()) {
          client.tableOperations().create(tableProps.name);
        }
      }

      for (var table : client.tableOperations().list()) {
        tablePropsBefore.put(table, client.tableOperations().getTableProperties(table));
      }
    }

    // test a dry run, should not fail and should not change anything
    try (var in = new ByteArrayInputStream(YAML1.getBytes(UTF_8))) {
      ImportConfigCommand.load(getServerContext(), in, DRY_RUN_OPTS);
    }

    // verify dry run did not change anything
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      for (var entry : RG_PROPS.entrySet()) {
        assertEquals(Map.of(), client.resourceGroupOperations().getProperties(entry.getKey()));
      }
      assertEquals(Map.of(),
          client.resourceGroupOperations().getProperties(ResourceGroupId.DEFAULT));

      for (var entry : NAMESPACE_PROPS.entrySet()) {
        assertEquals(Map.of(), client.namespaceOperations().getNamespaceProperties(entry.getKey()));
      }
      assertEquals(Map.of(), client.namespaceOperations().getNamespaceProperties(""));

      for (var tableProps : TABLE_CHANGES) {
        assertEquals(tablePropsBefore.get(tableProps.name()),
            client.tableOperations().getTableProperties(tableProps.name()));
      }
    }

    // Now actually do the import
    try (var in = new ByteArrayInputStream(YAML1.getBytes(UTF_8))) {
      ImportConfigCommand.load(getServerContext(), in, DEFAULT_OPTS);
    }

    assertEquals(YAML1, ExportConfigCommand.export(getServerContext()));

    // import export work directly on Zookeeper, ensure these changes are now visible via the API
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(SYSTEM_PROPS, client.instanceOperations().getSystemProperties());

      for (var entry : RG_PROPS.entrySet()) {
        assertEquals(entry.getValue(),
            client.resourceGroupOperations().getProperties(entry.getKey()));
      }
      assertEquals(Map.of(),
          client.resourceGroupOperations().getProperties(ResourceGroupId.DEFAULT));

      for (var entry : NAMESPACE_PROPS.entrySet()) {
        assertEquals(entry.getValue(),
            client.namespaceOperations().getNamespaceProperties(entry.getKey()));
      }
      assertEquals(Map.of(), client.namespaceOperations().getNamespaceProperties(""));

      for (var tableChanges : TABLE_CHANGES) {
        Map<String,String> expected = new TreeMap<>(tablePropsBefore.get(tableChanges.name()));
        if (tableChanges.noDefaults()) {
          expected.keySet().removeAll(IteratorConfigUtil.getInitialTableProperties().keySet());
        }
        expected.keySet().removeAll(tableChanges.delete());
        expected.putAll(tableChanges.props());

        tableChanges.iters().forEach(setting -> {
          for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
                scope.name().toLowerCase(), setting.getName());
            for (Map.Entry<String,String> prop : setting.getOptions().entrySet()) {
              expected.put(root + ".opt." + prop.getKey(), prop.getValue());
            }
            expected.put(root, setting.getPriority() + "," + setting.getIteratorClass());
          }
        });

        assertEquals(expected,
            new TreeMap<>(client.tableOperations().getTableProperties(tableChanges.name())));
      }

      // Test importing into a single table
      String singleTableImport = """
          scope: TABLE
          name: testing.customers
          properties: {
            table.iterator.scan.betaFilter: '150,ExperimentalFilter.class',
            table.iterator.scan.betaFilter.opt.dropMissing: 'false'
          }
          """;
      try (var in = new ByteArrayInputStream(singleTableImport.getBytes(UTF_8))) {
        ImportConfigCommand.load(getServerContext(), in, IGNORE_EXTRA_OPTS);
      }
      assertEquals(
          Map.of("table.iterator.scan.betaFilter", "150,ExperimentalFilter.class",
              "table.iterator.scan.betaFilter.opt.dropMissing", "false"),
          client.tableOperations().getTableProperties("testing.customers"));

      Set<String> tablesNotChanged = new HashSet<>(client.tableOperations().list());
      TABLE_CHANGES.forEach(tc -> tablesNotChanged.remove(tc.name()));
      for (var table : tablesNotChanged) {
        assertEquals(tablePropsBefore.get(table),
            client.tableOperations().getTableProperties(table));
      }

    }
  }

  @Test
  public void testMismatch() throws Exception {
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,Map<String,String>> tablePropsBefore = new HashMap<>();
      for (var entry : RG_PROPS.entrySet()) {
        client.resourceGroupOperations().create(entry.getKey());
      }

      for (var entry : NAMESPACE_PROPS.entrySet()) {
        client.namespaceOperations().create(entry.getKey());
      }

      for (var tableProps : TABLE_CHANGES) {
        if (tableProps.create()) {
          client.tableOperations().create(tableProps.name);
        }
      }
      for (var table : client.tableOperations().list()) {
        tablePropsBefore.put(table, client.tableOperations().getTableProperties(table));
      }

      for (var opts : List.of(DRY_RUN_OPTS, DEFAULT_OPTS)) {
        // delete a RG, should cause import to fail
        var rgid = RG_PROPS.keySet().iterator().next();
        client.resourceGroupOperations().remove(rgid);
        assertThrows(IllegalArgumentException.class, () -> {
          ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(YAML1.getBytes(UTF_8)), opts);
        });
        client.resourceGroupOperations().create(rgid);

        // delete a table
        var table = TABLE_CHANGES.get(0).name();
        client.tableOperations().delete(table);
        assertThrows(IllegalArgumentException.class, () -> {
          ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(YAML1.getBytes(UTF_8)), opts);
        });
        client.tableOperations().create(table);

        // add an extra rg not in the yaml
        client.resourceGroupOperations().create(ResourceGroupId.of("g123456789"));
        assertThrows(IllegalArgumentException.class, () -> {
          ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(YAML1.getBytes(UTF_8)), opts);
        });
        client.resourceGroupOperations().remove(ResourceGroupId.of("g123456789"));

        // add an extra namespace not in the yaml
        client.namespaceOperations().create("ns123456789");
        assertThrows(IllegalArgumentException.class, () -> {
          ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(YAML1.getBytes(UTF_8)), opts);
        });
        client.namespaceOperations().delete("ns123456789");

        // add an extra table not in the yaml
        client.tableOperations().create("ns123456789");
        assertThrows(IllegalArgumentException.class, () -> {
          ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(YAML1.getBytes(UTF_8)), opts);
        });
        client.tableOperations().delete("ns123456789");
      }
      // ensure nothing was changed by the failed imports
      for (var rgid2 : RG_PROPS.keySet()) {
        assertEquals(Map.of(), client.resourceGroupOperations().getProperties(rgid2));
      }

      for (var ns : NAMESPACE_PROPS.keySet()) {
        assertEquals(Map.of(), client.namespaceOperations().getNamespaceProperties(ns));
      }

      for (var table2 : client.tableOperations().list()) {
        assertEquals(tablePropsBefore.get(table2),
            client.tableOperations().getTableProperties(table2));
      }

      assertNotEquals(YAML1, ExportConfigCommand.export(getServerContext()));
      // import should succeed now
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream(YAML1.getBytes(UTF_8)),
          DEFAULT_OPTS);
      assertEquals(YAML1, ExportConfigCommand.export(getServerContext()));

      // create some extra stuff that is not in the yaml and try importing w/ allowExtra option
      client.resourceGroupOperations().create(ResourceGroupId.of("g123456789"));
      client.namespaceOperations().create("ns123456789");
      client.tableOperations().create("table123456789");
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream(YAML1.getBytes(UTF_8)),
          IGNORE_EXTRA_OPTS);
      // the exported yaml should have the extra stuff in it, so it should no longer be equals
      var newExport = ExportConfigCommand.export(getServerContext());
      assertNotEquals(YAML1, newExport);
      assertTrue(newExport.contains("g123456789"));
      assertTrue(newExport.contains("ns123456789"));
      assertTrue(newExport.contains("table123456789"));

    }
  }

  @Test
  public void testInvalid() throws Exception {
    // Ensure property validation runs on import and check that it runs for each scope.

    for (var opts : List.of(IGNORE_EXTRA_OPTS, DRY_RUN_IGNORE_EXTRA_OPTS)) {

      var tablePropsInSystem = """
          scope: SYSTEM
          name: ''
          properties: {
            table.cache.block.enable: 'true',
            table.cache.index.enable: 'true'
          }
          """;

      var iae = assertThrows(IllegalArgumentException.class, () -> {
        ImportConfigCommand.load(getServerContext(),
            new ByteArrayInputStream(tablePropsInSystem.getBytes(UTF_8)), opts);
      });
      assertTrue(iae.getMessage().contains("Set table properties at the namespace or table level"),
          iae::getMessage);

      var tablePropsInRG = """
          scope: RESOURCE_GROUP
          name: 'default'
          properties: {
            table.cache.block.enable: 'true',
            table.cache.index.enable: 'true'
          }
          """;

      iae = assertThrows(IllegalArgumentException.class, () -> {
        ImportConfigCommand.load(getServerContext(),
            new ByteArrayInputStream(tablePropsInRG.getBytes(UTF_8)), opts);
      });
      assertTrue(iae.getMessage().contains("Set table properties at the namespace or table level"),
          iae::getMessage);

      try (var client = Accumulo.newClient().from(getClientProps()).build()) {
        client.namespaceOperations().create("ievNS");
        client.tableOperations().create("ievNS.ievTable",
            new NewTableConfiguration().withoutDefaults());
      }

      var invalidNamespaceProps = """
          scope: NAMESPACE
          name: ievNS
          properties: {
            table.split.threshold: 'abc'
          }
          """;

      iae = assertThrows(IllegalArgumentException.class, () -> {
        ImportConfigCommand.load(getServerContext(),
            new ByteArrayInputStream(invalidNamespaceProps.getBytes(UTF_8)), opts);
      });
      assertTrue(iae.getMessage().contains("table.split.threshold"), iae::getMessage);

      var invalidTableProps = """
          scope: TABLE
          name: ievNS.ievTable
          properties: {
            table.split.threshold: 'abc'
          }
          """;

      iae = assertThrows(IllegalArgumentException.class, () -> {
        ImportConfigCommand.load(getServerContext(),
            new ByteArrayInputStream(invalidTableProps.getBytes(UTF_8)), opts);
      });
      assertTrue(iae.getMessage().contains("table.split.threshold"), iae::getMessage);

      // ensure nothing changed
      try (var client = Accumulo.newClient().from(getClientProps()).build()) {
        assertEquals(Map.of(), client.instanceOperations().getSystemProperties());
        assertEquals(Map.of(),
            client.resourceGroupOperations().getProperties(ResourceGroupId.DEFAULT));
        assertEquals(Map.of(), client.namespaceOperations().getNamespaceProperties("ievNS"));
        assertEquals(Map.of(), client.tableOperations().getTableProperties("ievNS.ievTable"));

        client.tableOperations().delete("ievNS.ievTable");
        client.namespaceOperations().delete("ievNS");
      }
    }

  }

  /**
   * Test when yaml has duplicate scope+name, should fail.
   */
  @Test
  public void testDuplicate() throws Exception {

    var template = """
        scope: __SCOPE__
        name: dup
        properties: {
          table.split.threshold: 5
        }
        ---
        scope: __SCOPE__
        name: dup
        properties: {
          table.split.threshold: 7
        }
        """;

    var correctTemplate = """
        scope: __SCOPE__
        name: dup
        properties: {
          table.split.threshold: 5
        }
        """;

    for (var scope : ExportConfigCommand.Scope.values()) {
      var yaml = template.replace("__SCOPE__", scope.name());
      var iae = assertThrows(IllegalArgumentException.class,
          () -> ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream(yaml.getBytes(UTF_8)), DRY_RUN_IGNORE_EXTRA_OPTS));
      assertEquals("Duplicate scope+name in input, scope:" + scope.name() + " name:dup",
          iae.getMessage());

      var opts = new ImportConfigCommand.Opts();
      opts.expectedFile = write(yaml);
      opts.dryRun = true;
      opts.ignoreExtra = true;
      var correctYaml = correctTemplate.replace("__SCOPE__", scope.name());
      iae = assertThrows(IllegalArgumentException.class, () -> ImportConfigCommand
          .load(getServerContext(), new ByteArrayInputStream(correctYaml.getBytes(UTF_8)), opts));
      assertEquals("Duplicate scope+name in expected file, scope:" + scope.name() + " name:dup",
          iae.getMessage());

    }

    // Test snake yaml settings dissallow duplicate map keys
    var dupMapKeys = """
        scope: SYSTEM
        scope: RESOURCE_GROUP
        name: ''
        name: "rgid1"
        properties: {
        }
        """;
    var opts = new ImportConfigCommand.Opts();
    opts.expectedFile = write(dupMapKeys);
    opts.dryRun = true;
    opts.ignoreExtra = true;
    assertThrows(DuplicateKeyException.class, () -> ImportConfigCommand.load(getServerContext(),
        new ByteArrayInputStream(dupMapKeys.getBytes(UTF_8)), opts));

  }

  private String write(String yaml) throws IOException {
    var expectedFile = Files.createTempFile(tempDir, "iet", "yaml");
    try (var out = Files.newBufferedWriter(expectedFile, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)) {
      out.write(yaml);
    }
    return expectedFile.toAbsolutePath().toString();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExpected(boolean precheckExpected) throws Exception {

    var opts = new ImportConfigCommand.Opts();
    opts.ignoreExtra = true;

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      assertEquals(Map.of(), client.instanceOperations().getSystemProperties());

      var expectedSystem = """
          scope: SYSTEM
          name: ''
          properties: {
          }
          """;
      var updateSystem1 = """
          scope: SYSTEM
          name: ''
          properties: {
            general.server.threadpool.size: 5
          }
          """;
      var updateSystem2 = """
          scope: SYSTEM
          name: ''
          properties: {
            general.micrometer.log.metrics: log4j2,
            general.micrometer.enabled: true
          }
          """;
      assertEquals(Map.of(), client.instanceOperations().getSystemProperties());
      opts.expectedFile = write(expectedSystem);
      opts.inputFile = write(updateSystem1);
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream("".getBytes(UTF_8)),
          opts, precheckExpected);
      Wait.waitFor(() -> Map.of("general.server.threadpool.size", "5")
          .equals(client.instanceOperations().getSystemProperties()));
      // running the command again should fail because the expected is not correct
      opts.expectedFile = write(expectedSystem);
      opts.inputFile = write(updateSystem2);
      var cme = assertThrows(ConcurrentModificationException.class,
          () -> ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream("".getBytes(UTF_8)), opts, precheckExpected));
      assertEquals(
          "Properties in scope:SYSTEM name: do not match the expected values. To diagnose, export current config to a new file and diff with expected file.",
          cme.getMessage());
      if (precheckExpected) {
        assertNull(cme.getCause());
      } else {
        // when a failure happens in the zookeeper atomic update will have an exception wrapping an
        // exception
        assertNotNull(cme.getCause());
        assertEquals(ConcurrentModificationException.class, cme.getCause().getClass());
      }

      // test dry run with expected
      opts.dryRun = true;
      if (precheckExpected) {
        cme = assertThrows(ConcurrentModificationException.class,
            () -> ImportConfigCommand.load(getServerContext(),
                new ByteArrayInputStream("".getBytes(UTF_8)), opts, precheckExpected));
        assertEquals(
            "Properties in scope:SYSTEM name: do not match the expected values. To diagnose, export current config to a new file and diff with expected file.",
            cme.getMessage());
      } else {
        // should not fail and should not change anything, this is testing the test code.
        ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream("".getBytes(UTF_8)),
            opts, precheckExpected);
      }
      opts.dryRun = false;

      // The properties should not have changed, give any changes that may have been erroneously
      // made time to propagate
      Thread.sleep(100);
      assertEquals(Map.of("general.server.threadpool.size", "5"),
          client.instanceOperations().getSystemProperties());

      // running the import again should succeed w/ the new expected file
      opts.expectedFile = write(updateSystem1);
      opts.inputFile = write(updateSystem2);
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream("".getBytes(UTF_8)),
          opts, precheckExpected);
      Wait.waitFor(() -> Map
          .of("general.micrometer.log.metrics", "log4j2", "general.micrometer.enabled", "true")
          .equals(client.instanceOperations().getSystemProperties()));

      // test resource groups, namespaces and tables
      ResourceGroupId rgid1 = ResourceGroupId.of("expectedRG");
      client.resourceGroupOperations().create(rgid1);
      assertEquals(Map.of(), client.resourceGroupOperations().getProperties(rgid1));
      client.resourceGroupOperations().setProperty(rgid1, "general.server.threadpool.size",
          "987654321");
      client.resourceGroupOperations().setProperty(rgid1, "general.micrometer.enabled", "false");
      client.namespaceOperations().create("expns");
      client.namespaceOperations().setProperty("expns", "table.file.max", "50");
      client.tableOperations().create("expns.t1", new NewTableConfiguration().withoutDefaults());
      client.tableOperations().setProperty("expns.t1", "table.split.threshold", "100M");
      var exportYaml = ExportConfigCommand.export(getServerContext());
      client.resourceGroupOperations().setProperty(rgid1, "general.micrometer.enabled", "true");
      opts.expectedFile = write(exportYaml);
      opts.inputFile = write(exportYaml.replace("9", "10"));
      cme = assertThrows(ConcurrentModificationException.class,
          () -> ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream("".getBytes(UTF_8)), opts, precheckExpected));
      assertEquals(
          "Properties in scope:RESOURCE_GROUP name:expectedRG do not match the expected values. To diagnose, export current config to a new file and diff with expected file.",
          cme.getMessage());
      // The properties should not have changed, give any changes that may have been erroneously
      // made time to propagate
      Thread.sleep(100);
      assertEquals(Map.of("general.server.threadpool.size", "987654321",
          "general.micrometer.enabled", "true"),
          client.resourceGroupOperations().getProperties(rgid1));
      // correct the expected file to work and run again
      client.resourceGroupOperations().setProperty(rgid1, "general.micrometer.enabled", "false");
      opts.expectedFile = write(exportYaml);
      opts.inputFile = write(exportYaml.replace("987654321", "10"));
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream("".getBytes(UTF_8)),
          opts, precheckExpected);
      Wait.waitFor(() -> Map
          .of("general.server.threadpool.size", "10", "general.micrometer.enabled", "false")
          .equals(client.resourceGroupOperations().getProperties(rgid1)));

      // check table scope
      exportYaml = ExportConfigCommand.export(getServerContext());
      client.tableOperations().setProperty("expns.t1", "table.split.threshold", "200M");
      opts.expectedFile = write(exportYaml);
      opts.inputFile = write(exportYaml.replace("100M", "600M"));
      cme = assertThrows(ConcurrentModificationException.class,
          () -> ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream("".getBytes(UTF_8)), opts, precheckExpected));
      assertEquals(
          "Properties in scope:TABLE name:expns.t1 do not match the expected values. To diagnose, export current config to a new file and diff with expected file.",
          cme.getMessage());
      assertEquals(Map.of("table.split.threshold", "200M"),
          client.tableOperations().getTableProperties("expns.t1"));
      // correct expected file
      opts.expectedFile = write(exportYaml.replace("100M", "200M"));
      ImportConfigCommand.load(getServerContext(), new ByteArrayInputStream("".getBytes(UTF_8)),
          opts, precheckExpected);
      Wait.waitFor(() -> Map.of("table.split.threshold", "600M")
          .equals(client.tableOperations().getTableProperties("expns.t1")));

      // check case where expected file is missing a section
      exportYaml = ExportConfigCommand.export(getServerContext());
      client.tableOperations().create("expns.t2", new NewTableConfiguration().withoutDefaults());
      // the export yaml does not contain the new table
      var exportYaml2 = ExportConfigCommand.export(getServerContext());
      opts.expectedFile = write(exportYaml);
      opts.inputFile = write(exportYaml2.replace("600M", "123M"));
      var iae = assertThrows(IllegalArgumentException.class,
          () -> ImportConfigCommand.load(getServerContext(),
              new ByteArrayInputStream("".getBytes(UTF_8)), opts, precheckExpected));
      assertEquals(
          "Scope+name present in input but not present in expected file, scope:TABLE name:expns.t2",
          iae.getMessage());
    }
  }

  @Test
  public void testOffline() throws Exception {
    // Ensures import/export can work when no accumulo server processes are running

    // create a table and set some props
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      var props = Map.of("table.split.threshold", "12345M");
      client.tableOperations().create("ieOffline",
          new NewTableConfiguration().withoutDefaults().setProperties(props));
    }

    // stop all the servers
    for (var serverType : ServerType.values()) {
      if (serverType == ServerType.ZOOKEEPER) {
        continue;
      }
      getClusterControl().stopAllServers(serverType);
    }

    // Do export, edit, import
    var yaml = ExportConfigCommand.export(getServerContext());
    assertTrue(yaml.contains("ieOffline") && yaml.contains("table.split.threshold: 12345M"), yaml);
    var newYaml = yaml.replace("table.split.threshold: 12345M", "table.split.threshold: 1234M");

    try (var in = new ByteArrayInputStream(newYaml.getBytes(UTF_8))) {
      ImportConfigCommand.load(getServerContext(), in, DEFAULT_OPTS);
    }

    // restart the servers
    for (var serverType : ServerType.values()) {
      getClusterControl().startAllServers(serverType);
    }

    // verify the tables props were updated
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      var expected = Map.of("table.split.threshold", "1234M");
      assertEquals(expected, client.tableOperations().getTableProperties("ieOffline"));
    }
  }
}
