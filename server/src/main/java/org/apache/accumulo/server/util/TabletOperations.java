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
package org.apache.accumulo.server.util;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.tabletserver.UniqueNameAllocator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TabletOperations {
  
  private static final Logger log = Logger.getLogger(TabletOperations.class);
  
  public static String createTabletDirectory(FileSystem fs, String tableDir, Text endRow) {
    String lowDirectory;
    
    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();
    
    while (true) {
      try {
        if (endRow == null) {
          lowDirectory = Constants.DEFAULT_TABLET_LOCATION;
          Path lowDirectoryPath = new Path(tableDir + lowDirectory);
          if (fs.exists(lowDirectoryPath) || fs.mkdirs(lowDirectoryPath))
            return lowDirectory;
          log.warn("Failed to create " + lowDirectoryPath + " for unknown reason");
        } else {
          lowDirectory = "/" + Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
          Path lowDirectoryPath = new Path(tableDir + lowDirectory);
          if (fs.exists(lowDirectoryPath))
            throw new IllegalStateException("Dir exist when it should not " + lowDirectoryPath);
          if (fs.mkdirs(lowDirectoryPath))
            return lowDirectory;
        }
      } catch (IOException e) {
        log.warn(e);
      }
      
      log.warn("Failed to create dir for tablet in table " + tableDir + " will retry ...");
      UtilWaitThread.sleep(3000);
      
    }
  }
  
  public static String createTabletDirectory(String tableDir, Text endRow) {
    while (true) {
      try {
        FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
        return createTabletDirectory(fs, tableDir, endRow);
      } catch (IOException e) {
        log.warn(e);
      }
      UtilWaitThread.sleep(3000);
    }
  }
}
