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
package org.apache.accumulo.core.client.admin;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.summary.SummarizerConfigurationUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.io.Text;

/**
 * This object stores table creation parameters. Currently includes: {@link TimeType}, whether to include default iterators, and user-specified initial
 * properties
 *
 * @since 1.7.0
 */
public class NewTableConfiguration {

  private static final TimeType DEFAULT_TIME_TYPE = TimeType.MILLIS;
  private TimeType timeType = DEFAULT_TIME_TYPE;

  private boolean limitVersion = true;

  private Map<String,String> properties = Collections.emptyMap();
  private Map<String,String> samplerProps = Collections.emptyMap();
  private Map<String,String> summarizerProps = Collections.emptyMap();
  private Map<String,String> localityProps = Collections.emptyMap();
  private Map<String,String> iteratorProps = Collections.emptyMap();

  private void checkDisjoint(Map<String,String> props, Map<String,String> derivedProps, String kind) {
    checkArgument(Collections.disjoint(props.keySet(), derivedProps.keySet()), "Properties and derived %s properties are not disjoint", kind);
  }

  /**
   * Configure logical or millisecond time for tables created with this configuration.
   *
   * @param tt
   *          the time type to use; defaults to milliseconds
   * @return this
   */
  public NewTableConfiguration setTimeType(TimeType tt) {
    checkArgument(tt != null, "TimeType is null");

    this.timeType = tt;
    return this;
  }

  /**
   * Retrieve the time type currently configured.
   *
   * @return the time type
   */
  public TimeType getTimeType() {
    return timeType;
  }

  /**
   * Currently the only default iterator is the {@link VersioningIterator}. This method will cause the table to be created without that iterator, or any others
   * which may become defaults in the future.
   *
   * @return this
   */
  public NewTableConfiguration withoutDefaultIterators() {
    this.limitVersion = false;
    return this;
  }

  /**
   * Sets additional properties to be applied to tables created with this configuration. Additional calls to this method replace properties set by previous
   * calls.
   *
   * @param props
   *          additional properties to add to the table when it is created
   * @return this
   */
  public NewTableConfiguration setProperties(Map<String,String> props) {
    checkArgument(props != null, "properties is null");
    checkDisjoint(props, samplerProps, "sampler");
    checkDisjoint(props, summarizerProps, "summarizer");
    this.properties = new HashMap<>(props);
    return this;
  }

  /**
   * Retrieves the complete set of currently configured table properties to be applied to a table when this configuration object is used.
   *
   * @return the current properties configured
   */
  public Map<String,String> getProperties() {
    Map<String,String> propertyMap = new HashMap<>();

    if (limitVersion) {
      propertyMap.putAll(IteratorUtil.generateInitialTableProperties(limitVersion));
    }

    propertyMap.putAll(summarizerProps);
    propertyMap.putAll(samplerProps);
    propertyMap.putAll(properties);
    propertyMap.putAll(iteratorProps);
    propertyMap.putAll(localityProps);
    return Collections.unmodifiableMap(propertyMap);
  }

  /**
   * Enable building a sample data set on the new table using the given sampler configuration.
   *
   * @since 1.8.0
   */
  public NewTableConfiguration enableSampling(SamplerConfiguration samplerConfiguration) {
    requireNonNull(samplerConfiguration);
    Map<String,String> tmp = new SamplerConfigurationImpl(samplerConfiguration).toTablePropertiesMap();
    checkDisjoint(properties, tmp, "sampler");
    this.samplerProps = tmp;
    return this;
  }

  /**
   * Enables creating summary statistics using {@link Summarizer}'s for the new table.
   *
   * @since 2.0.0
   */
  public NewTableConfiguration enableSummarization(SummarizerConfiguration... configs) {
    requireNonNull(configs);
    Map<String,String> tmp = SummarizerConfigurationUtil.toTablePropertiesMap(Arrays.asList(configs));
    checkDisjoint(properties, tmp, "summarizer");
    summarizerProps = tmp;
    return this;
  }

  /**
   * Configures a table's locality groups prior to initial table creation.
   *
   * Allows locality groups to be set prior to table creation. Additional calls to this method prior to table creation will overwrite previous locality group
   * mappings.
   *
   * @param groups
   *          mapping of locality group names to column families in the locality group
   *
   * @since 2.0.0
   *
   * @see TableOperations#setLocalityGroups
   */
  public void setLocalityGroups(Map<String,Set<Text>> groups) {
    // ensure locality groups do not overlap
    LocalityGroupUtil.ensureNonOverlappingGroups(groups);
    localityProps = new HashMap<>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      Set<Text> colFams = entry.getValue();
      String value = LocalityGroupUtil.encodeColumnFamilies(colFams);
      localityProps.put(Property.TABLE_LOCALITY_GROUP_PREFIX + entry.getKey(), value);
    }
    localityProps.put(Property.TABLE_LOCALITY_GROUPS.getKey(), groups.keySet().stream().collect(Collectors.joining(",")));
  }

  /**
   * Configure iterator settings for a table prior to its creation.
   *
   * Additional calls to this method before table creation will overwrite previous iterator settings.
   *
   * @param setting
   *          object specifying the properties of the iterator
   * @throws AccumuloException
   *           if a general error occurs
   *
   * @since 2.0.0
   */
  public void attachIterator(IteratorSetting setting) throws AccumuloException {
    attachIterator(setting, EnumSet.allOf(IteratorScope.class));
  }

  /**
   * Configure iterator settings for a table prior to its creation.
   *
   * @param setting
   *          object specifying the properties of the iterator
   * @param scopes
   *          enumerated set of iterator scopes
   * @throws AccumuloException
   *           if a general error occurs
   *
   * @since 2.0.0
   */
  public void attachIterator(IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException {
    checkArgument(setting != null, "setting is null");
    checkArgument(scopes != null, "scopes is null");
    if (iteratorProps.isEmpty()) {
      iteratorProps = new HashMap<>();
    }
    checkIteratorConflicts(setting, scopes);
    for (IteratorScope scope : scopes) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        iteratorProps.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      iteratorProps.put(root, setting.getPriority() + "," + setting.getIteratorClass());
    }
  }

  private void checkIteratorConflicts(IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException {
    checkArgument(setting != null, "setting is null");
    checkArgument(scopes != null, "scopes is null");
    for (IteratorScope scope : scopes) {
      String scopeStr = String.format("%s%s", Property.TABLE_ITERATOR_PREFIX, scope.name().toLowerCase());
      String nameStr = String.format("%s.%s", scopeStr, setting.getName());
      String optStr = String.format("%s.opt.", nameStr);
      Map<String,String> optionConflicts = new TreeMap<>();
      for (Entry<String,String> property : iteratorProps.entrySet()) {
        if (property.getKey().startsWith(scopeStr)) {
          if (property.getKey().equals(nameStr))
            throw new AccumuloException(new IllegalArgumentException("iterator name conflict for " + setting.getName() + ": " + property.getKey() + "="
                + property.getValue()));
          if (property.getKey().startsWith(optStr))
            optionConflicts.put(property.getKey(), property.getValue());
          if (property.getKey().contains(".opt."))
            continue;
          String parts[] = property.getValue().split(",");
          if (parts.length != 2)
            throw new AccumuloException("Bad value for existing iterator setting: " + property.getKey() + "=" + property.getValue());
          try {
            if (Integer.parseInt(parts[0]) == setting.getPriority())
              throw new AccumuloException(new IllegalArgumentException("iterator priority conflict: " + property.getKey() + "=" + property.getValue()));
          } catch (NumberFormatException e) {
            throw new AccumuloException("Bad value for existing iterator setting: " + property.getKey() + "=" + property.getValue());
          }
        }
      }
      if (optionConflicts.size() > 0)
        throw new AccumuloException(new IllegalArgumentException("iterator options conflict for " + setting.getName() + ": " + optionConflicts));
    }
  }

}
