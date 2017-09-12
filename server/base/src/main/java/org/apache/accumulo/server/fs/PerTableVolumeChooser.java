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
package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that delegates to another volume chooser based on other properties: table.custom.volume.chooser for tables, and
 * general.custom.scoped.volume.chooser for scopes. general.custor.{scope}.volume.chooser can override the system wide setting for
 * general.custom.scoped.volume.chooser. At the this this was written, the only known scope was "logger".
 */
public class PerTableVolumeChooser implements VolumeChooser {
  // TODO rename this class to DelegatingChooser? It delegates for more than just per-table scope
  private static final Logger log = LoggerFactory.getLogger(PerTableVolumeChooser.class);
  // TODO Add hint of expected size to construction, see ACCUMULO-3410
  /* Track VolumeChooser instances so they can keep state. */
  private final ConcurrentHashMap<Table.ID,VolumeChooser> tableSpecificChooserCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ChooserScope,VolumeChooser> scopeSpecificChooserCache = new ConcurrentHashMap<>();
  private final RandomVolumeChooser randomChooser = new RandomVolumeChooser();

  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory lazyConfFactory = null;

  public static final String TABLE_VOLUME_CHOOSER = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "volume.chooser";

  public static final String getPropertyNameForScope(ChooserScope scope) {
    return Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + scope.name().toLowerCase() + ".volume.chooser";
  }

  private static final String DEFAULT_SCOPED_VOLUME_CHOOSER = getPropertyNameForScope(ChooserScope.DEFAULT);

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) throws VolumeChooserException {
    log.trace("{}.choose", getClass().getSimpleName());
    return getDelegateChooser(env).choose(env, options);
  }

  // visible (not private) for testing
  VolumeChooser getDelegateChooser(VolumeChooserEnvironment env) {
    switch (env.getScope()) {
      case INIT:
        // TODO should be possible to read from SiteConfiguration during init
        log.warn("Not possible to determine delegate chooser at '{}' scope. Using {}.", ChooserScope.INIT, RandomVolumeChooser.class.getName());
        return randomChooser;
      case TABLE:
        return getVolumeChooserForTable(env, loadConfFactory());
      default:
        return getVolumeChooserForScope(env, loadConfFactory());
    }
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env, ServerConfigurationFactory confFactory) {
    log.trace("Looking up property {} for table id: {}", TABLE_VOLUME_CHOOSER, env.getTableId());
    final TableConfiguration tableConf = confFactory.getTableConfiguration(env.getTableId());
    String clazz = tableConf.get(TABLE_VOLUME_CHOOSER);

    // fall back to global default scope, so setting only one default is necessary, rather than a separate default for TABLE scope than other scopes
    if (null == clazz || clazz.isEmpty()) {
      clazz = confFactory.getSystemConfiguration().get(DEFAULT_SCOPED_VOLUME_CHOOSER);
    }

    if (null == clazz || clazz.isEmpty()) {
      String msg = "Property " + TABLE_VOLUME_CHOOSER + " or " + DEFAULT_SCOPED_VOLUME_CHOOSER + " must be a valid " + VolumeChooser.class.getSimpleName()
          + " to use the " + getClass().getSimpleName();
      throw new VolumeChooserException(msg);
    }

    String context = getTableContext(tableConf); // can be null
    return createVolumeChooser(context, clazz, TABLE_VOLUME_CHOOSER, env.getTableId(), tableSpecificChooserCache);
  }

  // visible (not private) for testing
  String getTableContext(TableConfiguration tableConf) {
    return tableConf.get(Property.TABLE_CLASSPATH);
  }

  private VolumeChooser getVolumeChooserForScope(VolumeChooserEnvironment env, ServerConfigurationFactory confFactory) {
    ChooserScope scope = env.getScope();
    String property = getPropertyNameForScope(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    AccumuloConfiguration systemConfiguration = confFactory.getSystemConfiguration();
    String clazz = systemConfiguration.get(property);

    // fall back to global default scope if this scope isn't configured (and not already default scope)
    if ((null == clazz || clazz.isEmpty()) && scope != ChooserScope.DEFAULT) {
      log.debug("{} not found; using {}", property, DEFAULT_SCOPED_VOLUME_CHOOSER);
      clazz = systemConfiguration.get(DEFAULT_SCOPED_VOLUME_CHOOSER);

      if (null == clazz || clazz.isEmpty()) {
        String msg = "Property " + property + " or " + DEFAULT_SCOPED_VOLUME_CHOOSER + " must be a valid " + VolumeChooser.class.getSimpleName()
            + " to use the " + getClass().getSimpleName();
        throw new VolumeChooserException(msg);
      }

      property = DEFAULT_SCOPED_VOLUME_CHOOSER;
    }

    String context = null;
    return createVolumeChooser(context, clazz, property, scope, scopeSpecificChooserCache);
  }

  /**
   * Create a volume chooser, using the cached version if any. This will replace the cached version if the class name has changed.
   *
   * @param clazz
   *          The volume chooser class name
   * @param property
   *          The property from which it was obtained
   * @param key
   *          The key to user in the cache
   * @param cache
   *          The cache
   * @return The volume chooser instance
   */
  private <T> VolumeChooser createVolumeChooser(String context, String clazz, String property, T key, ConcurrentHashMap<T,VolumeChooser> cache) {
    final String className = clazz.trim();
    // create a new instance, unless another thread beat us with one of the same class name, then use theirs
    return cache.compute(key, (k, previousChooser) -> {
      if (previousChooser != null && previousChooser.getClass().getName().equals(className)) {
        // no change; return the old one
        return previousChooser;
      } else if (previousChooser == null) {
        // TODO stricter definition of when the updated property is used, ref ACCUMULO-3412
        // don't log change if this is the first use
        log.trace("Change detected for {} for {}", property, key);
      }
      try {
        return ConfigurationTypeHelper.getClassInstance(context, className, VolumeChooser.class);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e) {
        String msg = "Failed to create instance for " + key + " configured to use " + className + " via " + property;
        throw new VolumeChooserException(msg, e);
      }
    });
  }

  // visible (not private) for testing
  ServerConfigurationFactory loadConfFactory() {
    // This local variable is an intentional component of the single-check idiom.
    ServerConfigurationFactory localConf = lazyConfFactory;
    if (localConf == null) {
      // If we're under contention when first getting here we'll throw away some initializations.
      localConf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
      lazyConfFactory = localConf;
    }
    return localConf;
  }

}
