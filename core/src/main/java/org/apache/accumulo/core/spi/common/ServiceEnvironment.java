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

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.data.TableId;

/**
 * This interface exposes Accumulo system level information to plugins in a stable manner. The
 * purpose of this interface is to insulate plugins from internal refactorings and changes to
 * Accumulo.
 *
 * <p>
 * Having this allows adding methods to the SPI which are not desired in PluginEnvironment which is
 * public API.
 *
 * @since 2.0.0
 */
public interface ServiceEnvironment extends PluginEnvironment {

  /**
   * @since 2.0.0
   */
  interface Configuration extends PluginEnvironment.Configuration {

  }

  /**
   * @return A view of Accumulo's system level configuration. This is backed by system level config
   *         in zookeeper, which falls back to site configuration, which falls back to the default
   *         configuration.
   */
  @Override
  Configuration getConfiguration();

  /**
   * @return a view of a table's configuration. When requesting properties that start with
   *         {@code table.} the returned configuration may give different values for different
   *         tables. For other properties the returned configuration will return the same value as
   *         {@link #getConfiguration()}.
   *
   */
  @Override
  Configuration getConfiguration(TableId tableId);
}
