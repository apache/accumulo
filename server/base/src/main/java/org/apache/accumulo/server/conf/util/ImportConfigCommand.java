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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.util.ExportConfigCommand.Scope;
import org.apache.accumulo.server.conf.util.ExportConfigCommand.ScopedProperties;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

@AutoService(KeywordExecutable.class)
public class ImportConfigCommand extends ServerKeywordExecutable<ImportConfigCommand.Opts> {

  private static final Logger log = LoggerFactory.getLogger(ImportConfigCommand.class);

  public ImportConfigCommand() {
    super(new Opts());
  }

  @Override
  public String keyword() {
    return "import";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.CONFIG;
  }

  @Override
  public String description() {
    return "Imports accumulo properties from a yaml file.";
  }

  public static class Opts extends ServerOpts {
    @Parameter(names = "--ignore-extra",
        description = "Proceed when Accumulo has extra tables, resource groups, or namespaces that are not in yaml")
    public boolean ignoreExtra = false;
    @Parameter(names = "--dry-run", description = "Only validates the yaml file, does not import.")
    public boolean dryRun = false;

  }

  static PropStoreKey getKey(Scope scope, String name, ServerContext context) {
    try {
      return switch (scope) {
        case SYSTEM -> SystemPropKey.of();
        case RESOURCE_GROUP -> ResourceGroupPropKey.of(ResourceGroupId.of(name));
        case NAMESPACE -> NamespacePropKey.of(context.getNamespaceId(name));
        case TABLE -> TablePropKey.of(context.getTableId(name));
      };
    } catch (NamespaceNotFoundException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  record ScopeName(Scope scope, String name) {
  }

  private static Set<ScopeName> getAllScopeNames(ServerContext context) {
    Set<ScopeName> all = new HashSet<>();

    all.add(new ScopeName(Scope.SYSTEM, ""));

    for (var rgid : context.resourceGroupOperations().list()) {
      all.add(new ScopeName(Scope.RESOURCE_GROUP, rgid.canonical()));
    }

    context.getNamespaceIdToNameMap().forEach((nsid, namespaceName) -> {
      all.add(new ScopeName(Scope.NAMESPACE, namespaceName));

      context.getTableMapping(nsid).createIdToQualifiedNameMap(namespaceName)
          .forEach((tableId, tableName) -> {
            all.add(new ScopeName(Scope.TABLE, tableName));
          });

    });

    return all;
  }

  private static void validate(ServerContext serverContext, List<ScopedProperties> allProps,
      boolean ignoreExtra) {
    var scopeNamesInYaml = new HashSet<ScopeName>();
    allProps.forEach(sp -> scopeNamesInYaml.add(new ScopeName(sp.scope(), sp.name())));
    var scopeNamesInAccumulo = getAllScopeNames(serverContext);

    if (!scopeNamesInYaml.equals(scopeNamesInAccumulo)) {
      boolean fail = false;
      for (var scopeName : Sets.difference(scopeNamesInYaml, scopeNamesInAccumulo)) {
        log.error("{}:{} is only in yaml and not present in Accumulo", scopeName.scope(),
            scopeName.name());
        fail = true;
      }
      if (!ignoreExtra) {
        for (var scopeName : Sets.difference(scopeNamesInAccumulo, scopeNamesInYaml)) {
          log.error("{}:{} is only in Accumulo and not present in yaml", scopeName.scope(),
              scopeName.name());
          fail = true;
        }
      }
      if (fail) {
        throw new IllegalArgumentException(
            "Yaml and Accumulo do not have the same tables,namespaces, and/or resource groups");
      }
    }

    // validate all scope+name before attempting to update any scope+name
    for (var scopedProps : allProps) {
      var propStoreKey = getKey(scopedProps.scope(), scopedProps.name(), serverContext);
      PropUtil.validateProperties(serverContext, propStoreKey, scopedProps.props());
    }
  }

  @VisibleForTesting
  public static void load(ServerContext serverContext, InputStream in, Opts options) {
    Yaml yaml = new Yaml();
    List<ScopedProperties> allProps = new ArrayList<>();
    for (var obj : yaml.loadAll(in)) {
      allProps.add(new ScopedProperties((Map<?,?>) obj));
    }

    validate(serverContext, allProps, options.ignoreExtra);

    if (!options.dryRun) {
      var propStore = serverContext.getPropStore();

      for (var sp : allProps) {
        var propStoreKey = getKey(sp.scope(), sp.name(), serverContext);
        propStore.replaceAll(propStoreKey, sp.props());
      }
    }
  }

  @Override
  public void execute(JCommander cl, Opts options) throws Exception {
    load(getServerContext(), System.in, options);
  }
}
