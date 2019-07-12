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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RandomVolumeChooser} that limits its choices from a given set of options to the subset
 * of those options preferred for a particular table. Defaults to selecting from all of the options
 * presented. Can be customized via the table property table.custom.volume.preferred, which should
 * contain a comma separated list of {@link Volume} URIs. Note that both the property name and the
 * format of its value are specific to this particular implementation.
 */
public class PreferredVolumeChooser extends RandomVolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PreferredVolumeChooser.class);

  private static final String TABLE_CUSTOM_SUFFIX = "volume.preferred";

  private static final String getCustomPropertySuffix(ChooserScope scope) {
    return "volume.preferred." + scope.name().toLowerCase();
  }

  private static final String DEFAULT_SCOPED_PREFERRED_VOLUMES =
      getCustomPropertySuffix(ChooserScope.DEFAULT);

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options)
      throws VolumeChooserException {
    log.trace("{}.choose", getClass().getSimpleName());
    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, getPreferredVolumes(env, options));
    log.trace("Choice = {}", choice);
    return choice;
  }

  // visible (not private) for testing
  String[] getPreferredVolumes(VolumeChooserEnvironment env, String[] options) {
    switch (env.getScope()) {
      case INIT:
        // TODO should be possible to read from SiteConfiguration during init
        log.warn("Not possible to determine preferred volumes at '{}' scope. Using all volumes.",
            ChooserScope.INIT);
        return options;
      case TABLE:
        return getPreferredVolumesForTable(env, options);
      default:
        return getPreferredVolumesForScope(env, options);
    }
  }

  private String[] getPreferredVolumesForTable(VolumeChooserEnvironment env, String[] options) {
    log.trace("Looking up property {} + for Table id: {}", TABLE_CUSTOM_SUFFIX, env.getTableId());

    String preferredVolumes =
        env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(TABLE_CUSTOM_SUFFIX);

    // fall back to global default scope, so setting only one default is necessary, rather than a
    // separate default for TABLE scope than other scopes
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);
    }

    // throw an error if volumes not specified or empty
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      String msg = "Property " + TABLE_CUSTOM_SUFFIX + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
          + " must be a subset of " + Arrays.toString(options) + " to use the "
          + getClass().getSimpleName();
      throw new VolumeChooserException(msg);
    }

    return parsePreferred(TABLE_CUSTOM_SUFFIX, preferredVolumes, options);
  }

  private String[] getPreferredVolumesForScope(VolumeChooserEnvironment env, String[] options) {
    ChooserScope scope = env.getScope();
    String property = getCustomPropertySuffix(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    String preferredVolumes = env.getServiceEnv().getConfiguration().getCustom(property);

    // fall back to global default scope if this scope isn't configured (and not already default
    // scope)
    if ((preferredVolumes == null || preferredVolumes.isEmpty()) && scope != ChooserScope.DEFAULT) {
      log.debug("{} not found; using {}", property, DEFAULT_SCOPED_PREFERRED_VOLUMES);
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);

      // only if the custom property is not set to we fall back to the default scoped preferred
      // volumes
      if (preferredVolumes == null || preferredVolumes.isEmpty()) {
        String msg = "Property " + property + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
            + " must be a subset of " + Arrays.toString(options) + " to use the "
            + getClass().getSimpleName();
        throw new VolumeChooserException(msg);
      }

      property = DEFAULT_SCOPED_PREFERRED_VOLUMES;
    }

    return parsePreferred(property, preferredVolumes, options);
  }

  private String[] parsePreferred(String property, String preferredVolumes, String[] options) {
    log.trace("Found {} = {}", property, preferredVolumes);

    Set<String> preferred = Arrays.stream(StringUtils.split(preferredVolumes, ','))
        .map(String::trim).collect(Collectors.toSet());
    if (preferred.isEmpty()) {
      String msg = "No volumes could be parsed from '" + property + "', which had a value of '"
          + preferredVolumes + "'";
      throw new VolumeChooserException(msg);
    }
    // preferred volumes should also exist in the original options (typically, from
    // instance.volumes)
    Set<String> optionsList = Arrays.stream(options).collect(Collectors.toSet());
    if (!preferred.stream().allMatch(optionsList::contains)) {
      String msg = "Some volumes in " + preferred + " are not valid volumes from " + optionsList;
      throw new VolumeChooserException(msg);
    }

    return preferred.toArray(new String[preferred.size()]);
  }
}
