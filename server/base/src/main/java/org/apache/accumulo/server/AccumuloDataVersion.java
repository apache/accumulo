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
package org.apache.accumulo.server;

import java.util.Set;

/**
 * Class representing the version of data stored in Accumulo.
 *
 * This version is separate but related to the file specific version in
 * {@link org.apache.accumulo.core.file.rfile.RFile}. A version change to RFile will reflect a
 * version change to the AccumuloDataVersion. But a version change to the AccumuloDataVersion may
 * not affect the version number in RFile. For example, changes made to other parts of Accumulo that
 * affects how data is stored, like the metadata table, would change the AccumuloDataVersion number
 * here but not in RFile.
 *
 * This number is stored in HDFS under {@link org.apache.accumulo.core.Constants#VERSION_DIR}.
 *
 * This class is used for checking the version during server startup and upgrades.
 */
public class AccumuloDataVersion {

  /**
   * version (10) reflects changes to how root tablet metadata is serialized in zookeeper starting
   * with 2.1. See {@link org.apache.accumulo.core.metadata.schema.RootTabletMetadata}.
   */
  public static final int ROOT_TABLET_META_CHANGES = 10;

  /**
   * version (9) reflects changes to crypto that resulted in RFiles and WALs being serialized
   * differently in version 2.0.0. Also RFiles in 2.0.0 may have summary data.
   */
  public static final int CRYPTO_CHANGES = 9;

  /**
   * version (8) reflects changes to RFile index (ACCUMULO-1124) AND the change to WAL tracking in
   * ZK in version 1.8.0
   */
  public static final int SHORTEN_RFILE_KEYS = 8;

  /**
   * Historic data versions
   *
   * <ul>
   * <li>version (7) also reflects the addition of a replication table in 1.7.0
   * <li>version (6) reflects the addition of a separate root table (ACCUMULO-1481) in 1.6.0 -
   * <li>version (5) moves delete file markers for the metadata table into the root tablet
   * <li>version (4) moves logging to HDFS in 1.5.0
   * </ul>
   */
  private static final int CURRENT_VERSION = ROOT_TABLET_META_CHANGES;

  /**
   * Get the current Accumulo Data Version. See Javadoc of static final integers for a detailed
   * description of that version.
   *
   * @return integer representing the Accumulo Data Version
   */
  public static int get() {
    return CURRENT_VERSION;
  }

  public static final Set<Integer> CAN_RUN =
      Set.of(SHORTEN_RFILE_KEYS, CRYPTO_CHANGES, CURRENT_VERSION);
}
