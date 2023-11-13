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
 * <p>
 * This version is separate but related to the file specific version in
 * {@link org.apache.accumulo.core.file.rfile.RFile}. A version change to RFile will reflect a
 * version change to the AccumuloDataVersion. But a version change to the AccumuloDataVersion may
 * not affect the version number in RFile. For example, changes made to other parts of Accumulo that
 * affects how data is stored, like the metadata table, would change the AccumuloDataVersion number
 * here but not in RFile.
 * <p>
 * This number is stored in HDFS under {@link org.apache.accumulo.core.Constants#VERSION_DIR}.
 * <p>
 * This class is used for checking the version during server startup and upgrades.
 */
public class AccumuloDataVersion {

  /**
   * version (12) reflect changes to support no chop merges including json encoding of the file
   * column family stored in root and metadata tables.
   */
  public static final int METADATA_FILE_JSON_ENCODING = 12;

  /**
   * version (11) reflects removal of replication starting with 3.0
   */
  public static final int REMOVE_DEPRECATIONS_FOR_VERSION_3 = 11;

  /**
   * version (10) reflects changes to how root tablet metadata is serialized in zookeeper starting
   * with 2.1. See {@link org.apache.accumulo.core.metadata.schema.RootTabletMetadata}.
   */
  public static final int ROOT_TABLET_META_CHANGES = 10;

  /**
   * Historic data versions
   *
   * <ul>
   * <li>version (9) RFiles and wal crypto serialization changes. RFile summary data in 2.0.0</li>
   * <li>version (8) RFile index (ACCUMULO-1124) and wal tracking in ZK in 1.8.0</li>
   * <li>version (7) also reflects the addition of a replication table in 1.7.0
   * <li>version (6) reflects the addition of a separate root table (ACCUMULO-1481) in 1.6.0 -
   * <li>version (5) moves delete file markers for the metadata table into the root tablet
   * <li>version (4) moves logging to HDFS in 1.5.0
   * </ul>
   */
  private static final int CURRENT_VERSION = METADATA_FILE_JSON_ENCODING;

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
      Set.of(ROOT_TABLET_META_CHANGES, REMOVE_DEPRECATIONS_FOR_VERSION_3, CURRENT_VERSION);

  /**
   * Get the stored, current working version.
   *
   * @param context the server context
   * @return the stored data version
   */
  public static int getCurrentVersion(ServerContext context) {
    int cv =
        context.getServerDirs().getAccumuloPersistentVersion(context.getVolumeManager().getFirst());
    ServerContext.ensureDataVersionCompatible(cv);
    return cv;
  }

  public static String oldestUpgradeableVersionName() {
    return dataVersionToReleaseName(CAN_RUN.stream().mapToInt(x -> x).min().orElseThrow());
  }

  private static String dataVersionToReleaseName(final int version) {
    switch (version) {
      case ROOT_TABLET_META_CHANGES:
        return "2.1.0";
      case REMOVE_DEPRECATIONS_FOR_VERSION_3:
        return "3.0.0";
      case METADATA_FILE_JSON_ENCODING:
        return "3.1.0";
    }
    throw new IllegalArgumentException("Unsupported data version " + version);
  }
}
