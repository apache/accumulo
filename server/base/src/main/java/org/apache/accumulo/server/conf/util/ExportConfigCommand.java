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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

@AutoService(KeywordExecutable.class)
public class ExportConfigCommand extends ServerKeywordExecutable<ExportConfigCommand.Opts> {

  public ExportConfigCommand() {
    super(new Opts());
  }

  @Override
  public String keyword() {
    return "export";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.CONFIG;
  }

  @Override
  public String description() {
    return "Exports all accumulo configuration to yaml with well defined sort order to support diff.";
  }

  public static class Opts extends ServerOpts {

  }

  // The order of the enums is the order they will be sorted in yaml
  public enum Scope {
    SYSTEM, RESOURCE_GROUP, NAMESPACE, TABLE
  }

  record ScopedProperties(Scope scope, String name, Map<String,String> props) {

    private static final String SCOPE_KEY = "scope";
    private static final String NAME_KEY = "name";
    private static final String PROPERTIES_KEY = "properties";
    private static final Set<String> KEYS = Set.of(SCOPE_KEY, NAME_KEY, PROPERTIES_KEY);

    private static SortedMap<String,String> extractProps(Map<?,?> map) {
      Map<?,?> tmp = (Map<?,?>) map.get(PROPERTIES_KEY);
      var props = new TreeMap<String,String>();
      tmp.forEach((k, v) -> {
        props.put((String) k, (String) v);
      });
      return props;
    }

    ScopedProperties(Map<?,?> map) {
      this(Scope.valueOf((String) map.get(SCOPE_KEY)), (String) map.get(NAME_KEY),
          extractProps(map));
      Preconditions.checkArgument(KEYS.equals(map.keySet()),
          "Unexpected keys in yaml : " + map.keySet());
    }

    Map<?,?> toMap() {
      // use a linked hash map to control the order of fields in the yaml
      var map = new LinkedHashMap<>();
      map.put(SCOPE_KEY, scope.name());
      map.put(NAME_KEY, name);
      // use a treemap so the properties are sorted in yaml
      map.put(PROPERTIES_KEY, new TreeMap<>(props));
      return map;
    }
  }

  static PropStoreKey getKey(Scope scope, String id) {
    return switch (scope) {
      case SYSTEM -> SystemPropKey.of();
      case RESOURCE_GROUP -> ResourceGroupPropKey.of(ResourceGroupId.of(id));
      case NAMESPACE -> NamespacePropKey.of(NamespaceId.of(id));
      case TABLE -> TablePropKey.of(TableId.of(id));
    };
  }

  private static ScopedProperties getProperties(PropStore propStore, Scope scope, String name) {
    return getProperties(propStore, scope, name, name);
  }

  private static ScopedProperties getProperties(PropStore propStore, Scope scope, String id,
      String name) {
    var vprops = propStore.get(getKey(scope, id));
    return new ScopedProperties(scope, name, vprops.asMap());
  }

  private static List<ScopedProperties> getAllProperties(ServerContext context) throws Exception {
    List<ScopedProperties> allProps = new ArrayList<>();

    var propStore = context.getPropStore();

    allProps.add(getProperties(propStore, Scope.SYSTEM, ""));

    for (var rgid : context.resourceGroupOperations().list()) {
      allProps.add(getProperties(propStore, Scope.RESOURCE_GROUP, rgid.canonical()));
    }

    context.getNamespaceIdToNameMap().forEach((nsid, namespaceName) -> {
      allProps.add(getProperties(propStore, Scope.NAMESPACE, nsid.canonical(), namespaceName));

      context.getTableMapping(nsid).createIdToQualifiedNameMap(namespaceName)
          .forEach((tableId, tableName) -> {
            allProps.add(getProperties(propStore, Scope.TABLE, tableId.canonical(), tableName));
          });

    });

    return allProps;
  }

  @VisibleForTesting
  public static String export(ServerContext context) throws Exception {
    List<ScopedProperties> allProps = getAllProperties(context);

    DumperOptions dopts = new DumperOptions();
    dopts.setPrettyFlow(true);
    var yaml = new Yaml(dopts);

    ToIntFunction<ScopedProperties> scopeToInt = sp -> sp.scope().ordinal();
    Comparator<ScopedProperties> comp =
        Comparator.comparingInt(scopeToInt).thenComparing(ScopedProperties::name);

    return yaml.dumpAll(allProps.stream().sorted(comp).map(ScopedProperties::toMap).iterator());
  }

  @Override
  public void execute(JCommander cl, Opts options) throws Exception {
    System.out.println(export(getServerContext()));
  }
}
