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

import java.io.IOException;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ServerConstants {
  
  // versions should never be negative
  public static final Integer WIRE_VERSION = 2;
  
  /**
   * current version reflects the addition of a separate root table (ACCUMULO-1481)
   */
  public static final int DATA_VERSION = 6;
  public static final int PREV_DATA_VERSION = 5;
  
  private static String[] baseDirs = null;
  private static String defaultBaseDir = null;

  public static synchronized String getDefaultBaseDir() {
    if (defaultBaseDir == null) {
      String singleNamespace = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
      String dfsUri = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_URI);
      String baseDir;
      
      if (dfsUri == null || dfsUri.isEmpty()) {
        Configuration hadoopConfig = CachedConfiguration.getInstance();
        try {
          baseDir = FileSystem.get(hadoopConfig).getUri().toString() + singleNamespace;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        baseDir = dfsUri + singleNamespace;
      }
      
      defaultBaseDir = new Path(baseDir).toString();
      
    }
    
    return defaultBaseDir;
  }

  // these are functions to delay loading the Accumulo configuration unless we must
  public static synchronized String[] getBaseDirs() {
    if (baseDirs == null) {
      String singleNamespace = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
      String ns = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES);
      
      if (ns == null || ns.isEmpty()) {
        baseDirs = new String[] {getDefaultBaseDir()};
      } else {
        String namespaces[] = ns.split(",");
        baseDirs = prefix(namespaces, singleNamespace);
      }
    }
    
    return baseDirs;
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
  
  public static final String TABLE_DIR = "tables";
  public static final String RECOVERY_DIR = "recovery";
  public static final String WAL_DIR = "wal";

  public static String[] getTablesDirs() {
    return prefix(getBaseDirs(), TABLE_DIR);
  }

  public static String[] getRecoveryDirs() {
    return prefix(getBaseDirs(), RECOVERY_DIR);
  }
  
  public static String[] getWalDirs() {
    return prefix(getBaseDirs(), WAL_DIR);
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
