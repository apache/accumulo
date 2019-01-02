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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.slf4j.LoggerFactory;

/**
 * This object stores table creation parameters. Currently includes: {@link TimeType}, whether to
 * include default iterators, and user-specified initial properties
 *
 * @since 1.7.0
 */
public class NewTableConfiguration {

  private static final TimeType DEFAULT_TIME_TYPE = TimeType.MILLIS;
  private TimeType timeType = DEFAULT_TIME_TYPE;

  private boolean limitVersion = true;

  private Map<String,String> properties = new HashMap<>();
  private SamplerConfiguration samplerConfiguration;

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
   * Currently the only default iterator is the {@link VersioningIterator}. This method will cause
   * the table to be created without that iterator, or any others which may become defaults in the
   * future.
   *
   * @return this
   */
  public NewTableConfiguration withoutDefaultIterators() {
    this.limitVersion = false;
    return this;
  }

  /**
   * Sets additional properties to be applied to tables created with this configuration. Additional
   * calls to this method replaces properties set by previous calls.
   *
   * @param prop
   *          additional properties to add to the table when it is created
   * @return this
   */
  public NewTableConfiguration setProperties(Map<String,String> prop) {
    checkArgument(prop != null, "properties is null");
    SamplerConfigurationImpl.checkDisjoint(prop, samplerConfiguration);

    try {
      LocalityGroupUtil.checkLocalityGroups(prop.entrySet());
    } catch (LocalityGroupConfigurationError e) {
      LoggerFactory.getLogger(NewTableConfiguration.class).warn(
          "Setting new table properties with bad locality group config.   Even though this warning"
              + " was displayed, the properties were set. props:" + prop,
          e);
    }

    this.properties = new HashMap<>(prop);
    return this;
  }

  /**
   * Retrieves the complete set of currently configured table properties to be applied to a table
   * when this configuration object is used.
   *
   * @return the current properties configured
   */
  public Map<String,String> getProperties() {
    Map<String,String> propertyMap = new HashMap<>();

    if (limitVersion) {
      propertyMap.putAll(IteratorUtil.generateInitialTableProperties(limitVersion));
    }

    if (samplerConfiguration != null) {
      propertyMap.putAll(new SamplerConfigurationImpl(samplerConfiguration).toTablePropertiesMap());
    }

    propertyMap.putAll(properties);
    return Collections.unmodifiableMap(propertyMap);
  }

  /**
   * Enable building a sample data set on the new table using the given sampler configuration.
   *
   * @since 1.8.0
   */
  public NewTableConfiguration enableSampling(SamplerConfiguration samplerConfiguration) {
    requireNonNull(samplerConfiguration);
    SamplerConfigurationImpl.checkDisjoint(properties, samplerConfiguration);
    this.samplerConfiguration = samplerConfiguration;
    return this;
  }
}
