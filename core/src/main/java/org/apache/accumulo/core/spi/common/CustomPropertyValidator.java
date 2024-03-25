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
package org.apache.accumulo.core.spi.common;

import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;

/**
 * Interface to be used on SPI classes where custom properties are in use. When
 * {@code #validateConfiguration(org.apache.accumulo.core.client.PluginEnvironment.Configuration)}
 * is called validation of the custom properties should be performed, if applicable. Implementations
 * should log any configuration issues at the warning level before finally returning false.
 *
 * @since 2.1.3
 */
public interface CustomPropertyValidator {

  public interface PropertyValidationEnvironment extends ServiceEnvironment {

    Optional<TableId> getTableId();

    /**
     * Some plugins in Accumulo support a map of options that go along with the plugin. If the
     * plugin being validated follows this pattern, then this method can help get its options.
     * Accumulo know what plugin it is currently validating and can therefore return appropriate
     * options for it.
     *
     * @return The options for the plugin being validated. This will return different options
     *         depending on which plugin is being validated. For example if
     *         {@link org.apache.accumulo.core.spi.compaction.CompactionDispatcher} is being
     *         validated then will return the same thing as
     *         {@link CompactionDispatcher.InitParameters#getOptions()}. If a
     *         {@link org.apache.accumulo.core.spi.scan.ScanDispatcher} plugin is being validated
     *         then this will return the same thing as
     *         {@link ScanDispatcher.InitParameters#getOptions()}
     * @throws IllegalArgumentException if the plugin type being validated does not have options.
     */
    Map<String,String> getPluginOptions();
  }

  /**
   * Validates implementation custom property configuration
   *
   * @param env PropertyValidationEnvironment instance
   * @return true if configuration is valid, else false after logging all warnings
   */
  public boolean validateConfiguration(PropertyValidationEnvironment env);

}
