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
package org.apache.accumulo.core;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Constants {
  // defines Accumulo data version constants
  public static final String VERSION = FilteredConstants.VERSION;
  public static final String VERSION_DIR = "version";
  public static final String APPNAME = "org.apache.accumulo";

  // important directories
  public static final String INSTANCE_ID_DIR = "instance_id";
  public static final String TABLE_DIR = "tables";
  public static final String RECOVERY_DIR = "recovery";
  public static final String WAL_DIR = "wal";

  // Zookeeper locations
  public static final String ZROOT = "/accumulo";
  public static final String ZINSTANCES = "/instances";

  public static final String ZTABLES = "/tables";
  public static final byte[] ZTABLES_INITIAL_ID = {'0'};
  public static final String ZTABLE_NAME = "/name";
  public static final String ZTABLE_DELETE_MARKER = "/deleting";
  public static final String ZTABLE_STATE = "/state";
  public static final String ZTABLE_FLUSH_ID = "/flush-id";
  public static final String ZTABLE_COMPACT_ID = "/compact-id";
  public static final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";
  public static final String ZTABLE_NAMESPACE = "/namespace";

  public static final String ZNAMESPACES = "/namespaces";
  public static final String ZNAMESPACE_NAME = "/name";

  public static final String ZMANAGERS = "/managers";
  public static final String ZMANAGER_LOCK = ZMANAGERS + "/lock";
  public static final String ZMANAGER_GOAL_STATE = ZMANAGERS + "/goal_state";
  public static final String ZMANAGER_REPLICATION_COORDINATOR_ADDR = ZMANAGERS + "/repl_coord_addr";
  public static final String ZMANAGER_TICK = ZMANAGERS + "/tick";

  public static final String ZGC = "/gc";
  public static final String ZGC_LOCK = ZGC + "/lock";

  public static final String ZMONITOR = "/monitor";
  public static final String ZMONITOR_LOCK = ZMONITOR + "/lock";
  public static final String ZMONITOR_HTTP_ADDR = ZMONITOR + "/http_addr";

  // used by < 2.1 table and namespace configurations
  public static final String ZCONF_LEGACY = "/conf";
  public static final String ZCONFIG = "/config";

  public static final String ZTSERVERS = "/tservers";

  public static final String ZSSERVERS = "/sservers";

  public static final String ZCOMPACTORS = "/compactors";

  public static final String ZCOORDINATOR = "/coordinators";
  public static final String ZCOORDINATOR_LOCK = ZCOORDINATOR + "/lock";

  public static final String ZDEAD = "/dead";
  public static final String ZDEADTSERVERS = ZDEAD + "/tservers";

  public static final String ZTRACERS = "/tracers";

  public static final String ZPROBLEMS = "/problems";

  public static final String BULK_ARBITRATOR_TYPE = "bulkTx";

  public static final String ZFATE = "/fate";

  public static final String ZNEXT_FILE = "/next_file";

  // TODO: Remove when Property.TSERV_WORKQ_THREADS is removed
  public static final String ZBULK_FAILED_COPYQ = "/bulk_failed_copyq";

  public static final String ZHDFS_RESERVATIONS = "/hdfs_reservations";
  public static final String ZRECOVERY = "/recovery";

  /**
   * Base znode for storing secret keys that back delegation tokens
   */
  public static final String ZDELEGATION_TOKEN_KEYS = "/delegation_token_keys";

  public static final String ZTABLE_LOCKS = "/table_locks";

  public static final String BULK_PREFIX = "b-";
  public static final String BULK_RENAME_FILE = "renames.json";
  public static final String BULK_LOAD_MAPPING = "loadmap.json";

  public static final String CLONE_PREFIX = "c-";
  public static final byte[] CLONE_PREFIX_BYTES = CLONE_PREFIX.getBytes(UTF_8);

  // this affects the table client caching of metadata
  public static final int SCAN_BATCH_SIZE = 1000;

  // Scanners will default to fetching 3 batches of Key/Value pairs before asynchronously
  // fetching the next batch.
  public static final long SCANNER_DEFAULT_READAHEAD_THRESHOLD = 3L;

  public static final int MAX_DATA_TO_PRINT = 64;
  public static final String CORE_PACKAGE_NAME = "org.apache.accumulo.core";
  public static final String MAPFILE_EXTENSION = "map";
  public static final String GENERATED_TABLET_DIRECTORY_PREFIX = "t-";

  public static final String EXPORT_METADATA_FILE = "metadata.bin";
  public static final String EXPORT_TABLE_CONFIG_FILE = "table_config.txt";
  public static final String EXPORT_FILE = "exportMetadata.zip";
  public static final String EXPORT_INFO_FILE = "accumulo_export_info.txt";
  public static final String IMPORT_MAPPINGS_FILE = "mappings.txt";

  public static final String HDFS_TABLES_DIR = "/tables";

  public static final int DEFAULT_VISIBILITY_CACHE_SIZE = 1000;
  public static final String DEFAULT_COMPACTION_SERVICE_NAME = "default";
}
