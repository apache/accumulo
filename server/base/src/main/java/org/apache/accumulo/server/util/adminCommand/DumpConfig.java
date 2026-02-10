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
package org.apache.accumulo.server.util.adminCommand;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.DumpConfig.DumpConfigOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.UsageGroup;
import org.apache.accumulo.start.spi.UsageGroups;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class DumpConfig extends ServerKeywordExecutable<DumpConfigOpts> {

  // This only exists because it is called from DumpConfigIT
  public static void main(String[] args) throws Exception {
    new DumpConfig().execute(args);
  }

  static class DumpConfigOpts extends ServerUtilOpts {
    @Parameter(names = {"-a", "--all"},
        description = "print the system and all table configurations")
    boolean allConfiguration = false;

    @Parameter(names = {"-d", "--directory"}, description = "directory to place config files")
    String directory = null;

    @Parameter(names = {"-s", "--system"}, description = "print the system configuration")
    boolean systemConfiguration = false;

    @Parameter(names = {"-rg", "--resourceGroups"},
        description = "print the resource group configuration")
    boolean resourceGroupConfiguration = false;

    @Parameter(names = {"-n", "--namespaces"}, description = "print the namespace configuration")
    boolean namespaceConfiguration = false;

    @Parameter(names = {"-t", "--tables"}, description = "print per-table configuration")
    List<String> tables = new ArrayList<>();

    @Parameter(names = {"-u", "--users"},
        description = "print users and their authorizations and permissions")
    boolean users = false;
  }

  private static final String ACCUMULO_SITE_BACKUP_FILE = "accumulo.properties.bak";
  private static final String NS_FILE_SUFFIX = "_ns.cfg";
  private static final String RG_FILE_SUFFIX = "_rg.cfg";
  private static final String USER_FILE_SUFFIX = "_user.cfg";
  private static final MessageFormat configFormat = new MessageFormat("config -t {0} -s {1}\n");
  private static final MessageFormat createNsFormat = new MessageFormat("createnamespace {0}\n");
  private static final MessageFormat createTableFormat = new MessageFormat("createtable {0}\n");
  private static final MessageFormat createUserFormat = new MessageFormat("createuser {0}\n");
  private static final MessageFormat createRGFormat =
      new MessageFormat("createresourcegroup {0}\n");
  private static final MessageFormat nsConfigFormat = new MessageFormat("config -ns {0} -s {1}\n");
  private static final MessageFormat rgConfigFormat = new MessageFormat("config -rg {0} -s {1}\n");
  private static final MessageFormat sysPermFormat =
      new MessageFormat("grant System.{0} -s -u {1}\n");
  private static final MessageFormat nsPermFormat =
      new MessageFormat("grant Namespace.{0} -ns {1} -u {2}\n");
  private static final MessageFormat tablePermFormat =
      new MessageFormat("grant Table.{0} -t {1} -u {2}\n");
  private static final MessageFormat userAuthsFormat =
      new MessageFormat("setauths -u {0} -s {1}\n");

  public DumpConfig() {
    super(new DumpConfigOpts());
  }

  @Override
  public String keyword() {
    return "dump-config";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Prints out non-default configuration settings.";
  }

  @Override
  public void execute(JCommander cl, DumpConfigOpts opts) throws Exception {

    ServerContext context = opts.getServerContext();

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
        justification = "app is run in same security context as user providing the filename")
    File outputDirectory = getOutputDirectory(opts.directory);
    DefaultConfiguration defaultConfig = DefaultConfiguration.getInstance();
    Map<String,String> siteConfig = context.instanceOperations().getSiteConfiguration();
    Map<String,String> systemConfig = context.instanceOperations().getSystemConfiguration();
    List<String> localUsers = null;
    if (opts.allConfiguration || opts.users) {
      localUsers = Lists.newArrayList(context.securityOperations().listLocalUsers());
      Collections.sort(localUsers);
    }

    if (opts.allConfiguration) {
      // print accumulo site
      printSystemConfiguration(outputDirectory, systemConfig, siteConfig, defaultConfig);
      // print resource groups
      for (ResourceGroupId group : context.resourceGroupOperations().list()) {
        printResourceGroupConfiguration(context, group, outputDirectory, systemConfig, siteConfig,
            defaultConfig);
      }
      // print namespaces
      for (String namespace : context.namespaceOperations().list()) {
        printNameSpaceConfiguration(context, namespace, outputDirectory, systemConfig, siteConfig,
            defaultConfig);
      }
      // print tables
      SortedSet<String> tableNames = context.tableOperations().list();
      for (String tableName : tableNames) {
        printTableConfiguration(context, tableName, outputDirectory, systemConfig, siteConfig,
            defaultConfig);
      }
      // print users
      if (localUsers != null) {
        for (String user : localUsers) {
          printUserConfiguration(context, user, outputDirectory);
        }
      }
    } else {
      if (opts.systemConfiguration) {
        printSystemConfiguration(outputDirectory, systemConfig, siteConfig, defaultConfig);
      }
      if (opts.resourceGroupConfiguration) {
        for (ResourceGroupId group : context.resourceGroupOperations().list()) {
          printResourceGroupConfiguration(context, group, outputDirectory, systemConfig, siteConfig,
              defaultConfig);
        }
      }
      if (opts.namespaceConfiguration) {
        for (String namespace : context.namespaceOperations().list()) {
          printNameSpaceConfiguration(context, namespace, outputDirectory, systemConfig, siteConfig,
              defaultConfig);
        }
      }
      if (!opts.tables.isEmpty()) {
        for (String tableName : opts.tables) {
          printTableConfiguration(context, tableName, outputDirectory, systemConfig, siteConfig,
              defaultConfig);
        }
      }
      if (opts.users && localUsers != null) {
        for (String user : localUsers) {
          printUserConfiguration(context, user, outputDirectory);
        }
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "app is run in same security context as user providing the filename")
  private File getOutputDirectory(final String directory) {
    if (directory == null) {
      return null;
    }
    Path outputDirectory = Path.of(directory);
    if (!Files.isDirectory(outputDirectory)) {
      throw new IllegalArgumentException(directory + " does not exist on the local filesystem.");
    }
    if (!Files.isWritable(outputDirectory)) {
      throw new IllegalArgumentException(directory + " is not writable");
    }
    return outputDirectory.toFile();
  }

  private String getDefaultConfigValue(DefaultConfiguration defaultConfig, String key) {
    if (key == null) {
      return null;
    }

    String defaultValue = null;
    try {
      Property p = Property.getPropertyByKey(key);
      if (p == null) {
        return defaultValue;
      }
      defaultValue = defaultConfig.get(p);
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return defaultValue;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printResourceGroupConfiguration(AccumuloClient accumuloClient, ResourceGroupId group,
      File outputDirectory, Map<String,String> systemConfig, Map<String,String> siteConfig,
      DefaultConfiguration defaultConfig) throws IOException, AccumuloException,
      AccumuloSecurityException, ResourceGroupNotFoundException {
    Path rgScript = outputDirectory.toPath().resolve(group + RG_FILE_SUFFIX);
    try (BufferedWriter nsWriter = Files.newBufferedWriter(rgScript)) {
      nsWriter.write(createRGFormat.format(new String[] {group.canonical()}));
      Map<String,String> props =
          ImmutableSortedMap.copyOf(accumuloClient.resourceGroupOperations().getProperties(group));
      for (Entry<String,String> entry : props.entrySet()) {
        String defaultValue = getDefaultConfigValue(defaultConfig, entry.getKey());
        if (defaultValue == null || !defaultValue.equals(entry.getValue())) {
          if (!entry.getValue().equals(siteConfig.get(entry.getKey()))
              && !entry.getValue().equals(systemConfig.get(entry.getKey()))) {
            nsWriter.write(rgConfigFormat
                .format(new String[] {group.canonical(), entry.getKey() + "=" + entry.getValue()}));
          }
        }
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printNameSpaceConfiguration(AccumuloClient accumuloClient, String namespace,
      File outputDirectory, Map<String,String> systemConfig, Map<String,String> siteConfig,
      DefaultConfiguration defaultConfig)
      throws IOException, AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    Path namespaceScript = outputDirectory.toPath().resolve(namespace + NS_FILE_SUFFIX);
    try (BufferedWriter nsWriter = Files.newBufferedWriter(namespaceScript)) {
      nsWriter.write(createNsFormat.format(new String[] {namespace}));
      Map<String,String> props = ImmutableSortedMap
          .copyOf(accumuloClient.namespaceOperations().getConfiguration(namespace));
      for (Entry<String,String> entry : props.entrySet()) {
        String defaultValue = getDefaultConfigValue(defaultConfig, entry.getKey());
        if (defaultValue == null || !defaultValue.equals(entry.getValue())) {
          if (!entry.getValue().equals(siteConfig.get(entry.getKey()))
              && !entry.getValue().equals(systemConfig.get(entry.getKey()))) {
            nsWriter.write(nsConfigFormat
                .format(new String[] {namespace, entry.getKey() + "=" + entry.getValue()}));
          }
        }
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printUserConfiguration(AccumuloClient accumuloClient, String user,
      File outputDirectory) throws IOException, AccumuloException, AccumuloSecurityException {
    Path userScript = outputDirectory.toPath().resolve(user + USER_FILE_SUFFIX);
    try (BufferedWriter userWriter = Files.newBufferedWriter(userScript)) {
      userWriter.write(createUserFormat.format(new String[] {user}));
      Authorizations auths = accumuloClient.securityOperations().getUserAuthorizations(user);
      userWriter.write(userAuthsFormat.format(new String[] {user, auths.toString()}));
      for (SystemPermission sp : SystemPermission.values()) {
        if (accumuloClient.securityOperations().hasSystemPermission(user, sp)) {
          userWriter.write(sysPermFormat.format(new String[] {sp.name(), user}));
        }
      }
      for (String namespace : accumuloClient.namespaceOperations().list()) {
        for (NamespacePermission np : NamespacePermission.values()) {
          if (accumuloClient.securityOperations().hasNamespacePermission(user, namespace, np)) {
            userWriter.write(nsPermFormat.format(new String[] {np.name(), namespace, user}));
          }
        }
      }
      for (String tableName : accumuloClient.tableOperations().list()) {
        for (TablePermission perm : TablePermission.values()) {
          if (accumuloClient.securityOperations().hasTablePermission(user, tableName, perm)) {
            userWriter.write(tablePermFormat.format(new String[] {perm.name(), tableName, user}));
          }
        }
      }
    }
  }

  private void printSystemConfiguration(File outputDirectory, Map<String,String> systemConfig,
      Map<String,String> siteConfig, DefaultConfiguration defaultConfig) throws IOException {
    TreeMap<String,String> conf = new TreeMap<>();
    TreeMap<String,String> site = new TreeMap<>(siteConfig);
    for (Entry<String,String> prop : site.entrySet()) {
      String defaultValue = getDefaultConfigValue(defaultConfig, prop.getKey());
      if (!prop.getValue().equals(defaultValue) && !systemConfig.containsKey(prop.getKey())) {
        conf.put(prop.getKey(), prop.getValue());
      }
    }
    TreeMap<String,String> system = new TreeMap<>(systemConfig);
    for (Entry<String,String> prop : system.entrySet()) {
      String defaultValue = getDefaultConfigValue(defaultConfig, prop.getKey());
      if (!prop.getValue().equals(defaultValue)) {
        conf.put(prop.getKey(), prop.getValue());
      }
    }
    Path siteBackup = outputDirectory.toPath().resolve(ACCUMULO_SITE_BACKUP_FILE);
    try (BufferedWriter fw = Files.newBufferedWriter(siteBackup)) {
      for (Entry<String,String> prop : conf.entrySet()) {
        fw.write(prop.getKey() + "=" + prop.getValue() + "\n");
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  private void printTableConfiguration(AccumuloClient accumuloClient, String tableName,
      File outputDirectory, Map<String,String> systemConfig, Map<String,String> siteConfig,
      DefaultConfiguration defaultConfig)
      throws AccumuloException, TableNotFoundException, IOException {
    Path tableBackup = outputDirectory.toPath().resolve(tableName + ".cfg");
    try (BufferedWriter writer = Files.newBufferedWriter(tableBackup)) {
      writer.write(createTableFormat.format(new String[] {tableName}));
      Map<String,String> props =
          ImmutableSortedMap.copyOf(accumuloClient.tableOperations().getConfiguration(tableName));
      for (Entry<String,String> prop : props.entrySet()) {
        if (prop.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
          String defaultValue = getDefaultConfigValue(defaultConfig, prop.getKey());
          if (defaultValue == null || !defaultValue.equals(prop.getValue())) {
            if (!prop.getValue().equals(siteConfig.get(prop.getKey()))
                && !prop.getValue().equals(systemConfig.get(prop.getKey()))) {
              writer.write(configFormat
                  .format(new String[] {tableName, prop.getKey() + "=" + prop.getValue()}));
            }
          }
        }
      }
    }
  }

}
