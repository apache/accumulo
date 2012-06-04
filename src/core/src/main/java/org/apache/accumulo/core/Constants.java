/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class Constants {
  public static final String VERSION = "1.3.6";
  public static final int DATA_VERSION = 3;
  
  // Zookeeper locations
  public static final String ZROOT = "/accumulo";
  public static final String ZINSTANCES = "/instances";
  
  public static final String ZTABLES = "/tables";
  public static final byte[] ZTABLES_INITIAL_ID = new byte[] {'0'};
  public static final String ZTABLE_NAME = "/name";
  public static final String ZTABLE_CONF = "/conf";
  public static final String ZTABLE_STATE = "/state";
  
  public static final String ZROOT_TABLET = "/root_tablet";
  public static final String ZROOT_TABLET_LOCATION = ZROOT_TABLET + "/location";
  public static final String ZROOT_TABLET_FUTURE_LOCATION = ZROOT_TABLET + "/future_location";
  public static final String ZROOT_TABLET_LAST_LOCATION = ZROOT_TABLET + "/lastlocation";
  public static final String ZROOT_TABLET_WALOGS = ZROOT_TABLET + "/walogs";
  
  public static final String ZMASTERS = "/masters";
  public static final String ZMASTER_LOCK = ZMASTERS + "/lock";
  public static final String ZMASTER_GOAL_STATE = ZMASTERS + "/goal_state";
  public static final String ZGC = "/gc";
  public static final String ZGC_LOCK = ZGC + "/lock";
  
  public static final String ZCONFIG = "/config";
  
  public static final String ZTSERVERS = "/tservers";
  
  public static final String ZLOGGERS = "/loggers";
  
  public static final String ZTRACERS = "/tracers";
  
  public static final String ZPROBLEMS = "/problems";
  
  public static final String ZDOOMEDSERVERS = "/doomed";
  
  public static final String METADATA_TABLE_ID = "!0";
  public static final String METADATA_TABLE_NAME = "!METADATA";
  public static final String DEFAULT_TABLET_LOCATION = "/default_tablet";
  public static final String TABLE_TABLET_LOCATION = "/table_info";
  
  // reserved keyspace is any row that begins with a tilde '~' character
  public static final Key METADATA_RESERVED_KEYSPACE_START_KEY = new Key(new Text(new byte[] {'~'}));
  public static final Key METADATA_RESERVED_KEYSPACE_STOP_KEY = new Key(new Text(new byte[] {'~' + 1}));
  public static final Range METADATA_RESERVED_KEYSPACE = new Range(METADATA_RESERVED_KEYSPACE_START_KEY, true, METADATA_RESERVED_KEYSPACE_STOP_KEY, false);
  public static final String METADATA_DELETE_FLAG_PREFIX = "~del";
  public static final Range METADATA_DELETES_KEYSPACE = new Range(new Key(new Text(METADATA_DELETE_FLAG_PREFIX)), true, new Key(new Text("~dem")), false);
  
  public static final Text METADATA_SERVER_COLUMN_FAMILY = new Text("srv");
  public static final Text METADATA_TABLET_COLUMN_FAMILY = new Text("~tab"); // this needs to sort after all other column families for that tablet
  public static final Text METADATA_CURRENT_LOCATION_COLUMN_FAMILY = new Text("loc");
  public static final Text METADATA_FUTURE_LOCATION_COLUMN_FAMILY = new Text("future");
  public static final Text METADATA_LAST_LOCATION_COLUMN_FAMILY = new Text("last");
  
  // README : very important that prevRow sort last to avoid race conditions between
  // garbage collector and split
  public static final ColumnFQ METADATA_PREV_ROW_COLUMN = new ColumnFQ(METADATA_TABLET_COLUMN_FAMILY, new Text("~pr")); // this needs to sort after everything
                                                                                                                        // else for that tablet
  public static final ColumnFQ METADATA_OLD_PREV_ROW_COLUMN = new ColumnFQ(METADATA_TABLET_COLUMN_FAMILY, new Text("oldprevrow"));
  public static final ColumnFQ METADATA_DIRECTORY_COLUMN = new ColumnFQ(METADATA_SERVER_COLUMN_FAMILY, new Text("dir"));
  public static final ColumnFQ METADATA_TIME_COLUMN = new ColumnFQ(METADATA_SERVER_COLUMN_FAMILY, new Text("time"));
  public static final ColumnFQ METADATA_SPLIT_RATIO_COLUMN = new ColumnFQ(METADATA_TABLET_COLUMN_FAMILY, new Text("splitRatio"));
  public static final ColumnFQ METADATA_LOCK_COLUMN = new ColumnFQ(METADATA_SERVER_COLUMN_FAMILY, new Text("lock"));
  
  public static final Text METADATA_DATAFILE_COLUMN_FAMILY = new Text("file");
  public static final Text METADATA_SCANFILE_COLUMN_FAMILY = new Text("scan");
  public static final Text METADATA_LOG_COLUMN_FAMILY = new Text("log");
  
  public static final Range METADATA_KEYSPACE = new Range(new Key(new Text(METADATA_TABLE_ID)), true, METADATA_RESERVED_KEYSPACE_START_KEY, false);
  
  public static final KeyExtent ROOT_TABLET_EXTENT = new KeyExtent(new Text(METADATA_TABLE_ID), KeyExtent.getMetadataEntry(new Text(METADATA_TABLE_ID), null),
      null);
  
  public static final String VALUE_ENCODING = "UTF-8";
  
  // note: all times are in milliseconds
  
  public static final int SCAN_BATCH_SIZE = 1000; // this affects the table client caching of metadata
  
  public static final long MIN_MASTER_LOOP_TIME = 1000;
  public static final int MASTER_TABLETSERVER_CONNECTION_TIMEOUT = 3000;
  public static final long CLIENT_SLEEP_BEFORE_RECONNECT = 1000;
  
  // Security configuration
  public static final String PW_HASH_ALGORITHM = "SHA-256";
  
  // Representation of an empty set of authorizations
  // (used throughout the code, because scans of metadata table and many tests do not set record-level visibility)
  public static final Authorizations NO_AUTHS = new Authorizations();
  
  public static final int DEFAULT_MINOR_COMPACTION_MAX_SLEEP_TIME = 60 * 3; // in seconds
  
  public static final int MAX_DATA_TO_PRINT = 64;
  public static final int CLIENT_RETRIES = 5;
  public static final int TSERV_MINC_MAXCONCURRENT_NUMWAITING_MULTIPLIER = 2;
  public static final String CORE_PACKAGE_NAME = "org.apache.accumulo.core";
  public static final String OLD_PACKAGE_NAME = "cloudbase";
  public static final String VALID_TABLE_NAME_REGEX = "^\\w+$";
  
  // these are functions to delay loading the Accumulo configuration unless we must
  public static String getBaseDir() {
    return AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_DFS_DIR);
  }
  
  public static String getTablesDir() {
    return getBaseDir() + "/tables";
  }
  
  public static String getRecoveryDir() {
    return getBaseDir() + "/recovery";
  }
  
  public static Path getInstanceIdLocation() {
    return new Path(getBaseDir() + "/instance_id");
  }
  
  public static Path getDataVersionLocation() {
    return new Path(getBaseDir() + "/version");
  }
  
  public static String getMetadataTableDir() {
    return getTablesDir() + "/" + METADATA_TABLE_ID;
  }
  
  public static String getRootTabletDir() {
    return getMetadataTableDir() + ZROOT_TABLET;
  }
}
