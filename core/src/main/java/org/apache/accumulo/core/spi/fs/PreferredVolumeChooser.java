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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.CustomPropertyValidator;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.accumulo.core.volume.Volume;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RandomVolumeChooser} that selects preferred volumes to use from the provided volume
 * options. Preferred Volumes are set on either the per-table, per-scope, or default configuration
 * level. Configuration details are overridden based on the top-down "Default","Site","System"
 * scopes.
 * <p>
 * Defaults to selecting a volume at random from the provided volume options.
 *
 * <p>
 * Property Details:
 * <p>
 * Preferred volumes can be set on a per-table basis via the custom property
 * {@code volume.preferred}.
 *
 * <p>
 * This property should contain a comma separated list of {@link Volume} URIs. Since this is a
 * custom property, it can be accessed under the prefix {@code table.custom}.<br>
 *
 * The {@code volume.preferred} property can be set at various configuration levels depending on the
 * scope. Note: Both the property name and the format of its value are specific to this particular
 * implementation.
 * <table border="1">
 * <caption>Scope Property Lookups and Defaults locations</caption>
 * <tr>
 * <th>Scope</th>
 * <th>Property Value Lookup</th>
 * <th>Default Property Lookup</th>
 * </tr>
 * <tr>
 * <td>{@code Scope.DEFAULT}</td>
 * <td>{@code general.custom.volume.preferred.default}</td>
 * <td>Throws RuntimeException if not set</td>
 * </tr>
 * <tr>
 * <td>{@code Scope.INIT}</td>
 * <td>{@code general.custom.volume.preferred.init}</td>
 * <td>{@code general.custom.volume.preferred.default}</td>
 * </tr>
 * <tr>
 * <td>{@code Scope.LOGGER}</td>
 * <td>{@code general.custom.volume.preferred.logger}</td>
 * <td>{@code general.custom.volume.preferred.default}</td>
 * </tr>
 * <tr>
 * <td>{@code Scope.TABLE}</td>
 * <td>{@code table.custom.volume.preferred}</td>
 * <td>{@code general.custom.volume.preferred.default}</td>
 * </tr>
 * </table>
 * <br>
 * <p>
 * Examples of expected usage:
 * <ul>
 * <li>Separate metadata table and write ahead logs from general data location.
 *
 * <pre>
 * <code>
 * // Set list of volumes
 * instance.volumes=hdfs://namenode_A/accumulo,hdfs://namenode_B/general
 * // Enable the preferred volume chooser
 * general.volume.chooser=org.apache.accumulo.core.spi.fs.PreferredVolumeChooser
 * // Set default preferred volume
 * general.custom.volume.preferred.default=hdfs://namenode_B/general
 * // Set write-ahead log preferred volume
 * general.custom.volume.preferred.logger=hdfs://namenode_A/accumulo
 * // Initialize and start accumulo
 * // Once accumulo is up, open the shell and configure the metadata table to use a preferred volume
 * config -t accumulo.metadata -s table.custom.volume.preferred=hdfs://namenode_A/accumulo
 * </code>
 * </pre>
 *
 * </li>
 * <li>Allow general data to use all volumes, but limit a specific table to a preferred volume
 *
 * <pre>
 * <code>
 * // Set list of volumes
 * instance.volumes=hdfs://namenode/accumulo,hdfs://namenode/accumulo_select
 * // Enable the preferred volume chooser
 * general.volume.chooser=org.apache.accumulo.core.spi.fs.PreferredVolumeChooser
 * // Set default preferred volumes
 * general.custom.volume.preferred.default=hdfs://namenode/accumulo,hdfs://namenode/accumulo_select
 * // Initialize and start accumulo
 * // Once accumulo is up, open the shell and configure the metadata table to use a preferred volume
 * config -t accumulo.metadata -s table.custom.volume.preferred=hdfs://namenode/accumulo_select
 * </code>
 * </pre>
 *
 * </li>
 * </ul>
 *
 * @since 2.1.0
 */
public class PreferredVolumeChooser extends RandomVolumeChooser implements CustomPropertyValidator {
  private static final Logger log = LoggerFactory.getLogger(PreferredVolumeChooser.class);

  private static final String TABLE_CUSTOM_SUFFIX = "volume.preferred";

  private static final String getCustomPropertySuffix(Scope scope) {
    return "volume.preferred." + scope.name().toLowerCase();
  }

  private static final String DEFAULT_SCOPED_PREFERRED_VOLUMES =
      getCustomPropertySuffix(Scope.DEFAULT);

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    log.trace("{}.choose", getClass().getSimpleName());
    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, getPreferredVolumes(env, options));
    log.trace("Choice = {}", choice);
    return choice;
  }

  /**
   *
   * Returns the subset of volumes that match the defined preferred volumes value
   *
   * @param env the server environment provided by the calling framework
   * @param options the subset of volumes to choose from
   * @return an array of preferred volumes that are a subset of {@link Property#INSTANCE_VOLUMES}
   */
  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options) {
    return getPreferredVolumes(env, options);
  }

  // visible (not private) for testing
  Set<String> getPreferredVolumes(VolumeChooserEnvironment env, Set<String> options) {
    if (env.getChooserScope() == Scope.TABLE) {
      return getPreferredVolumesForTable(env, options);
    }
    return getPreferredVolumesForScope(env, options);
  }

  private Set<String> getPreferredVolumesForTable(VolumeChooserEnvironment env,
      Set<String> options) {
    log.trace("Looking up property {} + for Table id: {}", TABLE_CUSTOM_SUFFIX, env.getTable());

    String preferredVolumes = env.getServiceEnv().getConfiguration(env.getTable().orElseThrow())
        .getTableCustom(TABLE_CUSTOM_SUFFIX);

    // fall back to global default scope, so setting only one default is necessary, rather than a
    // separate default for TABLE scope than other scopes
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);
    }

    // throw an error if volumes not specified or empty
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      String msg = "Property " + TABLE_CUSTOM_SUFFIX + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
          + " must be a subset of " + options + " to use the " + getClass().getSimpleName();
      throw new RuntimeException(msg);
    }

    return parsePreferred(TABLE_CUSTOM_SUFFIX, preferredVolumes, options);
  }

  private Set<String> getPreferredVolumesForScope(VolumeChooserEnvironment env,
      Set<String> options) {
    Scope scope = env.getChooserScope();
    String property = getCustomPropertySuffix(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    String preferredVolumes = env.getServiceEnv().getConfiguration().getCustom(property);

    // fall back to global default scope if this scope isn't configured (and not already default
    // scope)
    if ((preferredVolumes == null || preferredVolumes.isEmpty()) && scope != Scope.DEFAULT) {
      log.debug("{} not found; using {}", property, DEFAULT_SCOPED_PREFERRED_VOLUMES);
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);

      // only if the custom property is not set to we fall back to the default scoped preferred
      // volumes
      if (preferredVolumes == null || preferredVolumes.isEmpty()) {
        String msg = "Property " + property + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
            + " must be a subset of " + options + " to use the " + getClass().getSimpleName();
        throw new RuntimeException(msg);
      }

      property = DEFAULT_SCOPED_PREFERRED_VOLUMES;
    }

    return parsePreferred(property, preferredVolumes, options);
  }

  private Set<String> parsePreferred(String property, String preferredVolumes,
      Set<String> options) {
    log.trace("Found {} = {}", property, preferredVolumes);

    Set<String> preferred =
        Arrays.stream(preferredVolumes.split(",")).map(String::trim).collect(Collectors.toSet());
    if (preferred.isEmpty()) {
      String msg = "No volumes could be parsed from '" + property + "', which had a value of '"
          + preferredVolumes + "'";
      throw new RuntimeException(msg);
    }
    // preferred volumes should also exist in the original options (typically, from
    // instance.volumes)
    if (Collections.disjoint(preferred, options)) {
      String msg = "Some volumes in " + preferred + " are not valid volumes from " + options;
      throw new RuntimeException(msg);
    }

    return preferred;
  }

  @Override
  public boolean validateConfiguration(PropertyValidationEnvironment env) {

    final Set<String> options = new HashSet<>();
    String vols = env.getConfiguration().get(Property.INSTANCE_VOLUMES.getKey());
    for (String vol : vols.split(",")) {
      options.add(vol);
    }
    try {
      for (Scope scope : VolumeChooserEnvironment.Scope.values()) {
        this.getPreferredVolumes(new VolumeChooserEnvironment() {
          @Override
          public Text getEndRow() {
            return null;
          }

          @Override
          public Optional<TableId> getTable() {
            return env.getTableId();
          }

          @Override
          public Scope getChooserScope() {
            return scope;
          }

          @Override
          public ServiceEnvironment getServiceEnv() {
            return env;
          }
        }, options);
      }
      return true;
    } catch (RuntimeException e) {
      log.warn("Error validating properties", e);
      return false;
    }
  }
}
