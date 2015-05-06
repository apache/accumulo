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

import static org.apache.commons.lang.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

/**
 * A {@link RandomVolumeChooser} that limits its choices from a given set of options to the subset of those options preferred for a particular table. Defaults
 * to selecting from all of the options presented. Can be customized via the table property {@value #PREFERRED_VOLUMES_CUSTOM_KEY}, which should contain a comma
 * separated list of {@link Volume} URIs. Note that both the property name and the format of its value are specific to this particular implementation.
 */
public class PreferredVolumeChooser extends RandomVolumeChooser implements VolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PreferredVolumeChooser.class);

  /**
   * This should match {@link Property#TABLE_ARBITRARY_PROP_PREFIX}
   */
  public static final String PREFERRED_VOLUMES_CUSTOM_KEY = "table.custom.preferredVolumes";
  // TODO ACCUMULO-3417 replace this with the ability to retrieve by String key.
  private static final Predicate<String> PREFERRED_VOLUMES_FILTER = new Predicate<String>() {
    @Override
    public boolean apply(String key) {
      return PREFERRED_VOLUMES_CUSTOM_KEY.equals(key);
    }
  };

  @SuppressWarnings("unchecked")
  private final Map<String,Set<String>> parsedPreferredVolumes = Collections.synchronizedMap(new LRUMap(1000));
  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory serverConfs;

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    if (!env.hasTableId())
      return super.choose(env, options);

    // Get the current table's properties, and find the preferred volumes property
    // This local variable is an intentional component of the single-check idiom.
    ServerConfigurationFactory localConf = serverConfs;
    if (localConf == null) {
      // If we're under contention when first getting here we'll throw away some initializations.
      localConf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
      serverConfs = localConf;
    }
    TableConfiguration tableConf = localConf.getTableConfiguration(env.getTableId());
    final Map<String,String> props = new HashMap<String,String>();
    tableConf.getProperties(props, PREFERRED_VOLUMES_FILTER);
    if (props.isEmpty()) {
      log.warn("No preferred volumes specified. Defaulting to randomly choosing from instance volumes");
      return super.choose(env, options);
    }
    String volumes = props.get(PREFERRED_VOLUMES_CUSTOM_KEY);

    if (log.isTraceEnabled()) {
      log.trace("In custom chooser");
      log.trace("Volumes: " + volumes);
      log.trace("TableID: " + env.getTableId());
    }
    // If the preferred volumes property was specified, split the returned string by the comma and add use it to filter the given options.
    Set<String> preferred = parsedPreferredVolumes.get(volumes);
    if (preferred == null) {
      preferred = new HashSet<String>(Arrays.asList(StringUtils.split(volumes, ',')));
      parsedPreferredVolumes.put(volumes, preferred);
    }

    // Only keep the options that are in the preferred set
    final ArrayList<String> filteredOptions = new ArrayList<String>(Arrays.asList(options));
    filteredOptions.retainAll(preferred);

    // If there are no preferred volumes left, then warn the user and choose randomly from the instance volumes
    if (filteredOptions.isEmpty()) {
      log.warn("Preferred volumes are not instance volumes. Defaulting to randomly choosing from instance volumes");
      return super.choose(env, options);
    }

    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, filteredOptions.toArray(EMPTY_STRING_ARRAY));
    if (log.isTraceEnabled()) {
      log.trace("Choice = " + choice);
    }
    return choice;
  }
}
