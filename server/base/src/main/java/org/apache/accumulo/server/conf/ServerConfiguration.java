/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.conf;

import java.security.SecurityPermission;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.KeyExtent;

/**
 * Prefer {@link ServerConfigurationFactory} over this class, due to this class being deprecated in future versions with the addition of ACCUMULO-2615. It is
 * left un-deprecated here, due to the fact that doing so would generate too much churn and noise. See ACCUMULO-3019.
 */
public class ServerConfiguration {

  private static SecurityPermission CONFIGURATION_PERMISSION = new SecurityPermission("configurationPermission");

  public static synchronized SiteConfiguration getSiteConfiguration() {
    checkPermissions();
    return SiteConfiguration.getInstance(getDefaultConfiguration());
  }

  private static void checkPermissions() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(CONFIGURATION_PERMISSION);
    }
  }

  private static synchronized ZooConfiguration getZooConfiguration(Instance instance) {
    return (ZooConfiguration) new ServerConfigurationFactory(instance).getConfiguration();
  }

  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    checkPermissions();
    return DefaultConfiguration.getInstance();
  }

  public static synchronized AccumuloConfiguration getSystemConfiguration(Instance instance) {
    return getZooConfiguration(instance);
  }

  public static NamespaceConfiguration getNamespaceConfigurationForTable(Instance instance, String tableId) {
    return new ServerConfigurationFactory(instance).getNamespaceConfigurationForTable(tableId);
  }

  public static NamespaceConfiguration getNamespaceConfiguration(Instance instance, String namespaceId) {
    return new ServerConfigurationFactory(instance).getNamespaceConfiguration(namespaceId);
  }

  public static TableConfiguration getTableConfiguration(Instance instance, String tableId) {
    return new ServerConfigurationFactory(instance).getTableConfiguration(tableId);
  }

  static void expireAllTableObservers() {
    ServerConfigurationFactory.expireAllTableObservers();
  }

  private final ServerConfigurationFactory scf;

  public ServerConfiguration(Instance instance) {
    scf = new ServerConfigurationFactory(instance);
  }

  public TableConfiguration getTableConfiguration(String tableId) {
    return scf.getTableConfiguration(tableId);
  }

  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return getTableConfiguration(extent.getTableId().toString());
  }

  public NamespaceConfiguration getNamespaceConfiguration(String namespaceId) {
    return scf.getNamespaceConfiguration(namespaceId);
  }

  public synchronized AccumuloConfiguration getConfiguration() {
    return scf.getConfiguration();
  }

  public Instance getInstance() {
    return scf.getInstance();
  }
}
