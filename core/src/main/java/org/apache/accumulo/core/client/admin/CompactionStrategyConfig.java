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
package org.apache.accumulo.core.client.admin;

import static java.util.Objects.requireNonNull;

import java.util.Map;

/**
 * Configuration object which describes how a Compaction is run. Configuration objects are dependent
 * upon the CompactionStrategy running insider the server. This class is used in conjunction with
 * {@link CompactionConfig}.
 *
 * @since 1.7.0
 * @deprecated since 2.1.0 CompactionStrategies were deprecated for multiple reasons. First, they do
 *             not support the new compaction execution model. Second, they bind selection and
 *             output file configuration into a single entity when users need to configure these
 *             independently. Third, they use internal Accumulo types and ensuring their stability
 *             requires manual effort that may never happen. Fourth, writing a correct compaction
 *             strategy was exceedingly difficult as it required knowledge of internal tablet server
 *             synchronization in order to avoid causing scans to hang. Fifth although measure were
 *             taken to execute compaction strategies in the same manner as before, their execution
 *             in the new model has subtle differences that may result in suboptimal compactions.
 *             Please migrate to using {@link CompactionConfig#setSelector(PluginConfig)} and
 *             {@link CompactionConfig#setConfigurer(PluginConfig)} as soon as possible.
 */
@Deprecated(since = "2.1.0", forRemoval = true)
public class CompactionStrategyConfig {
  private String className;
  private Map<String,String> options = Map.of();

  /**
   * @param className The name of a class that implements
   *        org.apache.accumulo.tserver.compaction.CompactionStrategy. This class must exist on
   *        tservers.
   */
  public CompactionStrategyConfig(String className) {
    this.className = requireNonNull(className);
  }

  /**
   * @return the class name passed to the constructor.
   */
  public String getClassName() {
    return className;
  }

  /**
   * @param opts The options that will be passed to the init() method of the compaction strategy
   *        when its instantiated on a tserver. This method will copy the map. The default is an
   *        empty map.
   * @return this
   */
  public CompactionStrategyConfig setOptions(Map<String,String> opts) {
    this.options = Map.copyOf(opts);
    return this;
  }

  /**
   * @return The previously set options. Returns an unmodifiable map. The default is an empty map.
   */
  public Map<String,String> getOptions() {
    return options;
  }

  @Override
  public int hashCode() {
    return className.hashCode() + options.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionStrategyConfig) {
      CompactionStrategyConfig ocsc = (CompactionStrategyConfig) o;
      return className.equals(ocsc.className) && options.equals(ocsc.options);
    }

    return false;
  }
}
