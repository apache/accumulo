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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that delegates to another volume chooser based on other properties: table.custom.volume.chooser for tables, and
 * general.custom.scoped.volume.chooser for scopes. general.custor.{scope}.volume.chooser can override the system wide setting for
 * general.custom.scoped.volume.chooser. At the this this was written, the only known scope was "logger".
 */
public class PerTableVolumeChooser implements VolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PerTableVolumeChooser.class);
  // TODO Add hint of expected size to construction, see ACCUMULO-3410
  /* Track VolumeChooser instances so they can keep state. */
  private final ConcurrentHashMap<String,VolumeChooser> tableSpecificChooser = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,VolumeChooser> scopeSpecificChooser = new ConcurrentHashMap<>();
  private final RandomVolumeChooser randomChooser = new RandomVolumeChooser();

  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory serverConfs;

  public static final String INIT_SCOPE = "init";

  public static final String TABLE_VOLUME_CHOOSER = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "volume.chooser";

  public static final String SCOPED_VOLUME_CHOOSER(String scope) {
    return Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + scope + ".volume.chooser";
  }

  public static final String DEFAULT_SCOPED_VOLUME_CHOOSER = SCOPED_VOLUME_CHOOSER("scoped");

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    log.trace("PerTableVolumeChooser.choose");

    VolumeChooser chooser;
    if (!env.hasTableId() && (!env.hasScope() || env.getScope().equals(INIT_SCOPE))) {
      // Should only get here during Initialize. Configurations are not yet available.
      return randomChooser.choose(env, options);
    }

    ServerConfigurationFactory localConf = loadConf();
    if (env.hasTableId()) {
      // use the table configuration
      chooser = getVolumeChooserForTable(env, localConf);
    } else {
      // use the system configuration
      chooser = getVolumeChooserForNonTable(env, localConf);
    }

    return chooser.choose(env, options);
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) {
    log.trace("Looking up property {} for Table id: {}", TABLE_VOLUME_CHOOSER, env.getTableId());
    final TableConfiguration tableConf = localConf.getTableConfiguration(env.getTableId());
    String clazz = tableConf.get(TABLE_VOLUME_CHOOSER);

    if (null == clazz || clazz.isEmpty()) {
      String msg = "Property " + TABLE_VOLUME_CHOOSER + " must be set" + (null == clazz ? " " : " properly ") + "to use the " + getClass().getSimpleName();
      throw new RuntimeException(msg);
    }

    return createVolumeChooser(clazz, TABLE_VOLUME_CHOOSER, env.getTableId().canonicalID(), tableSpecificChooser);
  }

  private VolumeChooser getVolumeChooserForNonTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) {
    String scope = env.getScope();
    String property = SCOPED_VOLUME_CHOOSER(scope);

    log.trace("Looking up property: {}", property);

    AccumuloConfiguration systemConfiguration = localConf.getSystemConfiguration();
    String clazz = systemConfiguration.get(property);
    // only if the custom property is not set do we fallback to the default scope volume chooser setting
    if (null == clazz) {
      log.debug("Property not found: {} using {}", property, DEFAULT_SCOPED_VOLUME_CHOOSER);
      clazz = systemConfiguration.get(DEFAULT_SCOPED_VOLUME_CHOOSER);

      if (null == clazz || clazz.isEmpty()) {
        String msg = "Property " + property + " or " + DEFAULT_SCOPED_VOLUME_CHOOSER + " must be set" + (null == clazz ? " " : " properly ") + "to use the "
            + getClass().getSimpleName();
        throw new RuntimeException(msg);
      }

      property = DEFAULT_SCOPED_VOLUME_CHOOSER;
    }

    return createVolumeChooser(clazz, property, scope, scopeSpecificChooser);
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
  private VolumeChooser createVolumeChooser(String clazz, String property, String key, ConcurrentHashMap<String,VolumeChooser> cache) {
    VolumeChooser chooser = cache.get(key);
    // if we do not have a chooser or the class has changed, then create a new one
    if (chooser == null || !(chooser.getClass().getName().equals(clazz))) {
      if (chooser != null) {
        // TODO stricter definition of when the updated property is used, ref ACCUMULO-3412
        log.trace("Change detected for {} for {}", property, key);
      }
      // create a new volume chooser instance
      VolumeChooser temp;
      try {
        temp = loadClass(clazz);
      } catch (Exception e) {
        String msg = "Failed to create instance for " + key + " configured to use " + clazz + " via " + property;
        throw new RuntimeException(msg, e);
      }
      if (chooser == null) {
        // if we did not have one previously, then put this one in the cache
        // but use the one already in the cache if another thread beat us here
        chooser = cache.computeIfAbsent(key, k -> temp);
      } else {
        // otherwise the class has changed, so replace the one in the cache
        // unless another thread beat us here
        chooser = cache.computeIfPresent(key, (k, v) -> (v.getClass().getName().equals(clazz) ? v : temp));
      }
    }
    return chooser;
  }

  private VolumeChooser loadClass(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    // not attempting to load context because this approach to loading the class is for non-tables only
    return AccumuloVFSClassLoader.loadClass(className, VolumeChooser.class).newInstance();
  }

  private ServerConfigurationFactory loadConf() {
    // This local variable is an intentional component of the single-check idiom.
    ServerConfigurationFactory localConf = serverConfs;
    if (localConf == null) {
      // If we're under contention when first getting here we'll throw away some initializations.
      localConf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
      serverConfs = localConf;
    }
    return localConf;
  }

}
