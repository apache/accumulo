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
package org.apache.accumulo.server;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ServerConstants {
  
  // versions should never be negative
  public static final Integer WIRE_VERSION = 2;
  
  /**
   * current version reflects the addition of a separate root table (ACCUMULO-1481)
   */
  public static final int DATA_VERSION = 6;
  public static final int PREV_DATA_VERSION = 5;
  
  // these are functions to delay loading the Accumulo configuration unless we must
  public static String[] getBaseDirs() {
    String singleNamespace = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
    String ns = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES);
    if (ns == null || ns.isEmpty()) {
      Configuration hadoopConfig = CachedConfiguration.getInstance();
      String fullPath = hadoopConfig.get("fs.default.name") + singleNamespace;
      return new String[] {fullPath};
    }
    String namespaces[] = ns.split(",");
    if (namespaces.length < 2) {
      Configuration hadoopConfig = CachedConfiguration.getInstance();
      String fullPath = hadoopConfig.get("fs.default.name") + singleNamespace;
      return new String[] {fullPath};
    }
    return prefix(namespaces, singleNamespace);
  }
  
  public static String[] prefix(String bases[], String suffix) {
    if (suffix.startsWith("/"))
      suffix = suffix.substring(1);
    String result[] = new String[bases.length];
    for (int i = 0; i < bases.length; i++) {
      result[i] = bases[i] + "/" + suffix;
    }
    return result;
  }
  
  public static String[] getTablesDirs() {
    return prefix(getBaseDirs(), "tables");
  }
  
  public static String[] getRecoveryDirs() {
    return prefix(getBaseDirs(), "recovery");
  }
  
  public static String[] getWalDirs() {
    return prefix(getBaseDirs(), "wal");
  }
  
  public static String[] getWalogArchives() {
    return prefix(getBaseDirs(), "walogArchive");
  }
  
  public static Path getInstanceIdLocation() {
    return new Path(getBaseDirs()[0], "instance_id");
  }
  
  public static Path getDataVersionLocation() {
    return new Path(getBaseDirs()[0], "version");
  }
  
  public static String[] getRootTableDirs() {
    return prefix(getTablesDirs(), RootTable.ID);
  }
  
  public static String[] getMetadataTableDirs() {
    return prefix(getTablesDirs(), MetadataTable.ID);
  }
  
  public static String getRootTabletDir() {
    return prefix(getRootTableDirs(), RootTable.ROOT_TABLET_LOCATION)[0];
  }
}
