/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that delegates to another volume chooser based on other properties:
 * table.custom.volume.chooser for tables, and general.custom.volume.chooser.scoped for scopes.
 * general.custom.volume.chooser.{scope} can override the system wide setting for
 * general.custom.volume.chooser.scoped. At the this this was written, the only known scope was
 * "logger".
 */
public class PerTableVolumeChooser implements VolumeChooser {
  // TODO rename this class to DelegatingChooser? It delegates for more than just per-table scope
  private static final Logger log = LoggerFactory.getLogger(PerTableVolumeChooser.class);
  // TODO Add hint of expected size to construction, see ACCUMULO-3410
  /* Track VolumeChooser instances so they can keep state. */
  private final ConcurrentHashMap<TableId,VolumeChooser> tableSpecificChooserCache =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ChooserScope,VolumeChooser> scopeSpecificChooserCache =
      new ConcurrentHashMap<>();

  private static final String TABLE_CUSTOM_SUFFIX = "volume.chooser";

  private static final String getCustomPropertySuffix(ChooserScope scope) {
    return "volume.chooser." + scope.name().toLowerCase();
  }

  private static final String DEFAULT_SCOPED_VOLUME_CHOOSER =
      getCustomPropertySuffix(ChooserScope.DEFAULT);

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    log.trace("{}.choose", getClass().getSimpleName());
    return getDelegateChooser(env).choose(env, options);
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    return getDelegateChooser(env).choosable(env, options);
  }

  // visible (not private) for testing
  VolumeChooser getDelegateChooser(VolumeChooserEnvironment env) {
    if (env.getScope() == ChooserScope.TABLE) {
      return getVolumeChooserForTable(env);
    }
    return getVolumeChooserForScope(env);
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env) {
    log.trace("Looking up property {} for table id: {}", TABLE_CUSTOM_SUFFIX, env.getTableId());

    String clazz =
        env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(TABLE_CUSTOM_SUFFIX);

    // fall back to global default scope, so setting only one default is necessary, rather than a
    // separate default for TABLE scope than other scopes
    if (clazz == null || clazz.isEmpty()) {
      clazz = env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_VOLUME_CHOOSER);
    }

    if (clazz == null || clazz.isEmpty()) {
      String msg = "Property " + TABLE_CUSTOM_SUFFIX + " or " + DEFAULT_SCOPED_VOLUME_CHOOSER
          + " must be a valid " + VolumeChooser.class.getSimpleName() + " to use the "
          + getClass().getSimpleName();
      throw new VolumeChooserException(msg);
    }

    return createVolumeChooser(env, clazz, TABLE_CUSTOM_SUFFIX, env.getTableId(),
        tableSpecificChooserCache);
  }

  private VolumeChooser getVolumeChooserForScope(VolumeChooserEnvironment env) {
    ChooserScope scope = env.getScope();
    String property = getCustomPropertySuffix(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    String clazz = env.getServiceEnv().getConfiguration().getCustom(property);

    // fall back to global default scope if this scope isn't configured (and not already default
    // scope)
    if ((clazz == null || clazz.isEmpty()) && scope != ChooserScope.DEFAULT) {
      log.debug("{} not found; using {}", property, DEFAULT_SCOPED_VOLUME_CHOOSER);
      clazz = env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_VOLUME_CHOOSER);

      if (clazz == null || clazz.isEmpty()) {
        String msg =
            "Property " + property + " or " + DEFAULT_SCOPED_VOLUME_CHOOSER + " must be a valid "
                + VolumeChooser.class.getSimpleName() + " to use the " + getClass().getSimpleName();
        throw new VolumeChooserException(msg);
      }

      property = DEFAULT_SCOPED_VOLUME_CHOOSER;
    }

    return createVolumeChooser(env, clazz, property, scope, scopeSpecificChooserCache);
  }

  /**
   * Create a volume chooser, using the cached version if any. This will replace the cached version
   * if the class name has changed.
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
        throw new VolumeChooserException(msg, e);
      }
    });
  }
}
