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

import java.util.Optional;

import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TabletId;

/**
 * @since 4.0.0
 */
public interface TabletInformation {

  /**
   * Used to limit what information is obtained per tablet when calling
   * {@link TableOperations#getTabletInformation(String, RowRange, Field...)}
   *
   * @since 4.0.0
   */
  enum Field {
    LOCATION, FILES, AVAILABILITY, MERGEABILITY
  }

  /**
   * @return the TabletId for this tablet.
   */
  TabletId getTabletId();

  /**
   * Requires {@link Field#FILES} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return the number of files in the tablet directory.
   */
  int getNumFiles();

  /**
   * Requires {@link Field#FILES} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return the number of write-ahead logs associated with the tablet.
   */
  int getNumWalLogs();

  /**
   * Requires {@link Field#FILES} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return an estimated number of entries in the tablet.
   */
  long getEstimatedEntries();

  /**
   * Requires {@link Field#FILES} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return an estimated size of the tablet data on disk, which is likely the compressed size of
   *         the data.
   */
  long getEstimatedSize();

  /**
   * Requires {@link Field#LOCATION} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return the tablet hosting state.
   */
  String getTabletState();

  /**
   * Requires {@link Field#LOCATION} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return the Location of the tablet as a String or empty if the location in the TabletMetadata
   *         does not exist. When not empty, the String will be of the form
   *         "{@code <location type>:<host>:<port>}", where the location type is one of
   *         {@code CURRENT} or {@code FUTURE}
   */
  Optional<String> getLocation();

  /**
   * Requires {@link Field#FILES} to be specified at acquisition otherwise an exception will be
   * thrown.
   *
   * @return the directory name of the tablet.
   */
  String getTabletDir();

  /**
   * Requires {@link Field#AVAILABILITY} to be specified at acquisition otherwise an exception will
   * be thrown.
   *
   * @return the TabletAvailability object.
   */
  TabletAvailability getTabletAvailability();

  /**
   * Requires {@link Field#MERGEABILITY} to be specified at acquisition otherwise an exception will
   * be thrown.
   *
   * @return the TabletMergeabilityInfo object
   *
   * @since 4.0.0
   */
  TabletMergeabilityInfo getTabletMergeabilityInfo();
}
