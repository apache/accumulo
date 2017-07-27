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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RandomVolumeChooser} that limits its choices from a given set of options to the subset of those options preferred for a particular table. Defaults
 * to selecting from all of the options presented. Can be customized via the table property table.custom.preferredVolumes, which should contain a comma
 * separated list of {@link Volume} URIs. Note that both the property name and the format of its value are specific to this particular implementation.
 */
public class PreferredVolumeChooser extends RandomVolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PreferredVolumeChooser.class);

  public static final String INIT_SCOPE = "init";

  public static final String TABLE_PREFERRED_VOLUMES = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "preferred.volumes";

  public static final String SCOPED_PREFERRED_VOLUMES(String scope) {
    return Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + scope + ".preferred.volumes";
  }

  public static final String DEFAULT_SCOPED_PREFERRED_VOLUMES = SCOPED_PREFERRED_VOLUMES("scoped");

  @SuppressWarnings("unchecked")
  private final Map<String,Set<String>> parsedPreferredVolumes = Collections.synchronizedMap(new LRUMap(1000));
  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory serverConfs;

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    if (!env.hasTableId() && (!env.hasScope() || env.getScope().equals(INIT_SCOPE))) {
      // this should only happen during initialize
      log.warn("No table id or scope, so it's not possible to determine preferred volumes.  Using all volumes.");
      return super.choose(env, options);
    }

    // get the volumes property
    ServerConfigurationFactory localConf = loadConf();
    List<String> volumes;
    if (env.hasTableId()) {
      volumes = getPreferredVolumesForTable(env, localConf, options);
    } else {
      volumes = getPreferredVolumesForNonTable(env, localConf, options);
    }

    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, volumes.toArray(EMPTY_STRING_ARRAY));
    log.trace("Choice = {}", choice);

    return choice;
  }

  private List<String> getPreferredVolumesForTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf, String[] options) {
    log.trace("Looking up property {} + for Table id: {}", TABLE_PREFERRED_VOLUMES, env.getTableId());

    final TableConfiguration tableConf = localConf.getTableConfiguration(env.getTableId());
    String volumes = tableConf.get(TABLE_PREFERRED_VOLUMES);

    // throw an error if volumes not specified or empty
    if (null == volumes || volumes.isEmpty()) {
      String logmsg = "Missing or empty " + TABLE_PREFERRED_VOLUMES + " property";
      throw new RuntimeException(logmsg);
    }

    return getFilteredOptions(TABLE_PREFERRED_VOLUMES, volumes, options);
  }

  private List<String> getPreferredVolumesForNonTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf, String[] options) {
    String scope = env.getScope();
    String property = SCOPED_PREFERRED_VOLUMES(scope);

    log.trace("Looking up property: {}", property);

    AccumuloConfiguration systemConfiguration = localConf.getSystemConfiguration();
    String volumes = systemConfiguration.get(property);

    // only if the custom property is not set to we fallback to the default scoped preferred volumes
    if (null == volumes) {
      log.debug("Property not found: {} using {}", property, DEFAULT_SCOPED_PREFERRED_VOLUMES);
      volumes = systemConfiguration.get(DEFAULT_SCOPED_PREFERRED_VOLUMES);

      if (null == volumes || volumes.isEmpty()) {
        String logmsg = "Missing or empty " + property + " and " + DEFAULT_SCOPED_PREFERRED_VOLUMES + " properties";
        throw new RuntimeException(logmsg);
      }

      property = DEFAULT_SCOPED_PREFERRED_VOLUMES;
    }

    return getFilteredOptions(property, volumes, options);
  }

  private List<String> getFilteredOptions(String property, String volumes, String[] options) {
    log.trace("Found {} = {}", property, volumes);

    ArrayList<String> filteredOptions = getIntersection(options, volumes);

    // throw error if intersecting with preferred volumes resulted in the empty set
    if (filteredOptions.isEmpty()) {
      String logMessage = "After filtering preferred volumes, there are no valid instance volumes.";
      log.error(logMessage);
      throw new RuntimeException(logMessage);
    }

    return filteredOptions;
  }

  private ArrayList<String> getIntersection(String[] options, String volumes) {
    Set<String> preferred = parseVolumes(volumes);
    return filterWithPreferred(options, preferred);
  }

  private ArrayList<String> filterWithPreferred(String[] options, Set<String> preferred) {
    // Only keep the options that are in the preferred set
    final ArrayList<String> filteredOptions = new ArrayList<>(Arrays.asList(options));
    filteredOptions.retainAll(preferred);
    return filteredOptions;
  }

  private Set<String> parseVolumes(String volumes) {
    // If the preferred volumes property was specified, split the returned string by the comma and add use it to filter the given options.
    Set<String> preferred = parsedPreferredVolumes.get(volumes);
    if (preferred == null) {
      preferred = new HashSet<>(Arrays.asList(StringUtils.split(volumes, ',')));
      parsedPreferredVolumes.put(volumes, preferred);
    }
    return preferred;
  }

  private ServerConfigurationFactory loadConf() {
    // Get the current table's properties, and find the preferred volumes property
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
