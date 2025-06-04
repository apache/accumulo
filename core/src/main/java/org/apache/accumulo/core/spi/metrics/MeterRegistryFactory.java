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
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
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
   * Called on metrics initialization. Implementations should note the initial parameters set when
   * instantiating a MeterRegistry should be considered fixed. Once a MeterRegistry is initialized
   * parameters such as common tags may not be updated with later additions or changes.
   *
   * @return a Micrometer registry that will be added to the metrics configuration.
   */
  MeterRegistry create(final InitParameters params);
  /**
   * Description of what the function does.
   * @param patternList Description of what this variable is, i.e. comma-delimited regext patterns
   * @return description of what this function returns, i.e. a predicate
   */
  static MeterFilter getMeterFilter(String patternList) {
    Preconditions.checkNotNull(patternList, "patternList must not be null");
    Preconditions.checkArgument(!patternList.isEmpty(), "patternList must not be empty");

    String[] patterns = patternList.split(",");
    Predicate<Meter.Id> finalPredicate = null;

    for (String pattern : patterns) {
      // Compile the pattern.
      // Will throw PatternSyntaxException if invalid pattern.
      Pattern compiledPattern = Pattern.compile(pattern);

      // Create a predicate that will return true if the ID's name matches the pattern.
      Predicate<Meter.Id> predicate = id -> compiledPattern.matcher(id.getName()).matches();

      if (finalPredicate == null) {
        // This is the first pattern. Establish the initial predicate.
        finalPredicate = predicate;
      } else {
        // Conjoin the pattern into the final predicates. The final predicate will return true if the name of the ID matches any of its conjoined predicates.
        finalPredicate = finalPredicate.or(predicate);
      }
    }

    // Assert that meter filter reply == MeterFilterReply.DENY;
    return MeterFilter.deny(finalPredicate);
  }
}
