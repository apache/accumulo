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
package org.apache.accumulo.core.spi.metrics;

import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The Micrometer metrics allows for different monitoring systems. and can be enabled within
 * Accumulo with properties and are initialized by implementing this interface and providing the
 * factory implementation clas name as a property. Metrics are specified with the following
 * properties:
 * <p>
 * Property.GENERAL_MICROMETER_ENABLED = true
 * <p>
 * Property.GENERAL_MICROMETER_FACTORY = [implementation].class.getName()
 * <p>
 * Properties can be passed in the Accumulo properties files using the prefix
 * {@code general.custom.metrics.opts} for example. to set the polling rate used in the
 * {@link org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory} to 10 seconds use
 *
 * <pre>
 *     general.custom.metrics.opts.logging.step = 10s
 * </pre>
 *
 * Classes that implement custom properties will need to take care to implement unique names for
 * each factory, if multiple factories are used.
 *
 * @since 2.1.3
 */
public interface MeterRegistryFactory {
  // full form in property file is "general.custom.metrics.opts"
  String METRICS_PROP_SUBSTRING = "metrics.opts.";

  interface InitParameters {
    /**
     * Get the configured metrics properties passed as {@code general.custom.metrics.opts} The
     * returned map is the stripped names with {@code general.custom.metrics.opts} removed.
     * <p>
     * For example properties {@code general.custom.metrics.opts.prop1=abc} and
     * {@code general.custom.metrics.opts.prop9=123} are set, then this map would contain
     * {@code prop1=abc} and {@code prop9=123}.
     *
     * @return a map of property name, value pairs, stripped of a prefix.
     */
    Map<String,String> getOptions();

    /**
     * Optional extension point to pass additional information though the ServiceEnvironment.
     *
     * @return the service environment
     */
    ServiceEnvironment getServiceEnv();
  }

  /**
   * Set the initial parameters passed to the metrics factory to initialize the underlying metrics
   * registry that will be created by the factory with {@link #create()}
   *
   * @param params the initial parameters for the MetricsRegistry
   */
  default void setInitParams(InitParameters params) {}

  /**
   * Called on metrics initialization. Implementations should take care that the initial parameters
   * have been set before instantiating a MeterRegistry. Once a MeterRegistry is initialized
   * parameters such as common tags can be fixed and will not be updated with later additions or
   * changes.
   *
   * @return a Micrometer registry that will be added to the metrics configuration.
   */
  MeterRegistry create();
}
