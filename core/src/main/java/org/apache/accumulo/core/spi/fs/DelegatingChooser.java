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
package org.apache.accumulo.core.spi.fs;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that delegates to another volume chooser based on other properties:
 * table.custom.volume.chooser for tables, and general.custom.volume.chooser.scoped for scopes.
 * general.custom.volume.chooser.{scope} can override the system-wide setting for
 * general.custom.volume.chooser.scoped. At the time this was written, the only known scope was
 * "logger".
 *
 * @since 2.1.0
 */
public class DelegatingChooser implements VolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(DelegatingChooser.class);
  /* Track VolumeChooser instances so they can keep state. */
  private final ConcurrentHashMap<TableId,VolumeChooser> tableSpecificChooserCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Scope,VolumeChooser> scopeSpecificChooserCache =
      new ConcurrentHashMap<>();

  private static final String TABLE_CUSTOM_SUFFIX = "volume.chooser";

  private static final String getCustomPropertySuffix(Scope scope) {
    return "volume.chooser." + scope.name().toLowerCase();
  }

  private static final String DEFAULT_SCOPED_VOLUME_CHOOSER =
      getCustomPropertySuffix(Scope.DEFAULT);

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    log.trace("{}.choose", getClass().getSimpleName());
    return getDelegateChooser(env).choose(env, options);
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options) {
    return getDelegateChooser(env).choosable(env, options);
  }

  // visible (not private) for testing
  VolumeChooser getDelegateChooser(VolumeChooserEnvironment env) {
    if (env.getChooserScope() == Scope.TABLE) {
      return getVolumeChooserForTable(env);
    }
    return getVolumeChooserForScope(env);
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env) {
    log.trace("Looking up property {} for table id: {}", TABLE_CUSTOM_SUFFIX, env.getTable());

    String clazz = env.getServiceEnv().getConfiguration(env.getTable().get())
        .getTableCustom(TABLE_CUSTOM_SUFFIX);

    // fall back to global default scope, so setting only one default is necessary, rather than a
    // separate default for TABLE scope than other scopes
    if (clazz == null || clazz.isEmpty()) {
      clazz = env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_VOLUME_CHOOSER);
    }

    if (clazz == null || clazz.isEmpty()) {
      String msg = "Property " + Property.TABLE_ARBITRARY_PROP_PREFIX + TABLE_CUSTOM_SUFFIX + " or "
          + Property.GENERAL_ARBITRARY_PROP_PREFIX + DEFAULT_SCOPED_VOLUME_CHOOSER
          + " must be a valid " + VolumeChooser.class.getSimpleName() + " to use the "
          + getClass().getSimpleName();
      throw new RuntimeException(msg);
    }

    return createVolumeChooser(env, clazz, TABLE_CUSTOM_SUFFIX, env.getTable().get(),
        tableSpecificChooserCache);
  }

  private VolumeChooser getVolumeChooserForScope(VolumeChooserEnvironment env) {
    Scope scope = env.getChooserScope();
    String property = getCustomPropertySuffix(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    String clazz = env.getServiceEnv().getConfiguration().getCustom(property);

    // fall back to global default scope if this scope isn't configured (and not already default
    // scope)
    if ((clazz == null || clazz.isEmpty()) && scope != Scope.DEFAULT) {
      log.debug("{} not found; using {}", Property.TABLE_ARBITRARY_PROP_PREFIX + property,
          Property.GENERAL_ARBITRARY_PROP_PREFIX + DEFAULT_SCOPED_VOLUME_CHOOSER);
      clazz = env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_VOLUME_CHOOSER);

      if (clazz == null || clazz.isEmpty()) {
        String msg = "Property " + Property.TABLE_ARBITRARY_PROP_PREFIX + property + " or "
            + Property.GENERAL_ARBITRARY_PROP_PREFIX + DEFAULT_SCOPED_VOLUME_CHOOSER
            + " must be a valid " + VolumeChooser.class.getSimpleName() + " to use the "
            + getClass().getSimpleName();
        throw new RuntimeException(msg);
      }

      property = DEFAULT_SCOPED_VOLUME_CHOOSER;
    }

    return createVolumeChooser(env, clazz, property, scope, scopeSpecificChooserCache);
  }

  /**
   * Create a volume chooser, using the cached version if any. This will replace the cached version
   * if the class name has changed.
   *
   * @param clazz The volume chooser class name
   * @param property The property from which it was obtained
   * @param key The key to user in the cache
   * @param cache The cache
   * @return The volume chooser instance
   */
  private <T> VolumeChooser createVolumeChooser(VolumeChooserEnvironment env, String clazz,
      String property, T key, ConcurrentHashMap<T,VolumeChooser> cache) {
    final String className = clazz.trim();
    // create a new instance, unless another thread beat us with one of the same class name, then
    // use theirs
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
        if (key instanceof TableId) {
          TableId tableId = (TableId) key;
          return env.getServiceEnv().instantiate(tableId, className, VolumeChooser.class);
        } else {
          return env.getServiceEnv().instantiate(className, VolumeChooser.class);
        }
      } catch (Exception e) {
        String msg = "Failed to create instance for " + key + " configured to use " + className
            + " via " + property;
        throw new RuntimeException(msg, e);
      }
    });
  }
}
