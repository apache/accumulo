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
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.fs.Path;

import static org.apache.accumulo.core.Constants.*;

public class ServerConstants {
  // these are functions to delay loading the Accumulo configuration unless we must
  public static String getBaseDir() {
    return ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
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
