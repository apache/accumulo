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
package org.apache.accumulo.server.metrics;

import static org.apache.accumulo.core.conf.Property.GENERAL_ARBITRARY_PROP_PREFIX;
import static org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.METRICS_PROP_SUBSTRING;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;

/**
 * Provides a way to pass parameters from an Accumulo configuration to the MeterRegistryFactory.
 * Properties need have the form {@code general.custom.metrics.opts.PARAMETER_NAME = VALUE}. The
 * prefix {@code general.custom.metrics.opts.} is stripped and the resulting Map returned by
 * {@link #getOptions()} will be map of {@code PROPERTY_NAME, VALUE} key value pairs.
 * <p>
 * Other implementations can extend
 * {@link org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.InitParameters} to provide other
 * implementations
 */
public class MeterRegistryEnvPropImpl implements MeterRegistryFactory.InitParameters {

  private final ServerContext context;

  public MeterRegistryEnvPropImpl(final ServerContext context) {
    this.context = context;
  }

  /**
   * Properties that match {@code general.custom.metrics.opts.PARAMETER_NAME = VALUE} with be
   * filtered and returned with the prefix stripped.
   *
   * @return a map of the filtered, stripped property, value pairs.
   */
  @Override
  public Map<String,String> getOptions() {
    Map<String,String> filtered = new HashMap<>();

    Map<String,String> options = context.getConfiguration()
        .getAllPropertiesWithPrefixStripped(GENERAL_ARBITRARY_PROP_PREFIX);
    options.forEach((k, v) -> {
      if (k.startsWith(METRICS_PROP_SUBSTRING)) {
        String name = k.substring(METRICS_PROP_SUBSTRING.length());
        filtered.put(name, v);
      }
    });
    return filtered;
  }

  @Override
  public ServiceEnvironment getServiceEnv() {
    return new ServiceEnvironmentImpl(context);
  }
}
