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

import java.util.Set;

import org.apache.accumulo.core.clientImpl.ImportConfigurationImpl;

/**
 * A configuration object that can be used with
 * {@link TableOperations#importTable(String, Set, ImportConfiguration)}
 *
 * @since 2.1
 */
public interface ImportConfiguration {
  ImportConfiguration EMPTY = builder().build();

  /**
   * @return true if the new table is to be kept offline, false the table will be brought online.
   */
  boolean isKeepOffline();

  /**
   * See {@link Builder#setKeepMappings(boolean)} for full description.
   *
   * @return true if the {@link org.apache.accumulo.core.Constants#IMPORT_MAPPINGS_FILE} is to be
   *         kept after importing.
   */
  boolean isKeepMappings();

  /**
   * A ImportConfiguration builder
   *
   * @since 2.1
   */
  interface Builder {

    /**
     * The new table is normally brought online after the import process. This allows leaving the
     * new table offline
     *
     * @param keepOffline true if the new table is to be kept offline after importing.
     */
    Builder setKeepOffline(boolean keepOffline);

    /**
     * During the table import transaction, an intermediate
     * {@link org.apache.accumulo.core.Constants#IMPORT_MAPPINGS_FILE} file is created and usually
     * removed after the import process is completed. Setting this option to true will keep the file
     * after the import is finished. Typically, when this is set to true,
     * {@link #setKeepOffline(boolean)} is also set to true, allowing for validation/debugging
     * before bringing the new table online.
     *
     * @param keepMappings true if the
     *        {@link org.apache.accumulo.core.Constants#IMPORT_MAPPINGS_FILE} is to be kept after
     *        importing.
     */
    Builder setKeepMappings(boolean keepMappings);

    /**
     * Build the import table configuration
     *
     * @return the built immutable import table configuration
     */
    ImportConfiguration build();
  }

  /**
   * @return a {@link ImportConfiguration} builder
   */
  static ImportConfiguration.Builder builder() {
    return new ImportConfigurationImpl();
  }

  /**
   * @return an empty configuration object with the default settings.
   * @since 2.1.0
   */
  static ImportConfiguration empty() {
    return EMPTY;
  }

}
