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
package org.apache.accumulo.core.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TabletOperations {
  
  private static final Logger log = Logger.getLogger(TabletOperations.class);
  
  public static Map<Text,String> createNewTableTabletDirectories(FileSystem fs, String tableDir, Collection<Text> splitPoints) {
    
    // using this function is much more optimal than repeatedly calling createTabletDirectory()
    // because it makes a lot less calls to the name node
    
    Map<Text,String> tabletDirs = new TreeMap<Text,String>();
    
    synchronized (TabletOperations.class) {
      // assume master calls this and only one master is running, therefore
      // it is safe to synchronize in one process... if separate processes
      // call this there is a race condition between checking if a dir exist
      // and creating it
      while (true) {
        try {
          if (fs.exists(new Path(tableDir)))
            throw new RuntimeException("Table dir already exist " + tableDir);
          break;
        } catch (IOException ioe) {
          log.warn(ioe);
        }
        
        log.warn("Failed to check if table dir exist " + tableDir);
        UtilWaitThread.sleep(3000);
      }
      
      createDir(fs, tableDir);
    }
    
    HashSet<UUID> uuids = new HashSet<UUID>();
    
    for (Text splitPoint : splitPoints) {
      
      if (splitPoint == null) {
        createDir(fs, tableDir + Constants.DEFAULT_TABLET_LOCATION);
      } else {
        UUID uuid = UUID.nameUUIDFromBytes(TextUtil.getBytes(splitPoint));
        while (uuids.contains(uuid)) {
          uuid = UUID.randomUUID();
        }
        
        uuids.add(uuid);
        
        createDir(fs, tableDir + "/t-" + uuid.toString());
        
        tabletDirs.put(splitPoint, "/t-" + uuid.toString());
      }
      
    }
    
    return tabletDirs;
  }
  
  private static void createDir(FileSystem fs, String dir) {
    while (true) {
      try {
        if (fs.mkdirs(new Path(dir))) {
          break;
        }
      } catch (IOException ioe) {
        log.warn(ioe);
      }
      
      log.warn("Failed to create " + dir);
      UtilWaitThread.sleep(3000);
    }
  }
  
  public static String createTabletDirectory(FileSystem fs, String tableDir, Text endRow) {
    String lowDirectory;
    
    UUID uuid = UUID.randomUUID();
    
    while (true) {
      try {
        if (endRow == null) {
          lowDirectory = Constants.DEFAULT_TABLET_LOCATION;
          Path lowDirectoryPath = new Path(tableDir + lowDirectory);
          if (fs.mkdirs(lowDirectoryPath))
            return lowDirectory;
          log.warn("Failed to create " + lowDirectoryPath + " for unknown reason");
        } else {
          
          lowDirectory = "/t-" + uuid.toString();
          String lockFile = "/l-" + uuid.toString();
          Path lockPath = new Path(tableDir + lockFile);
          Path lowDirectoryPath = new Path(tableDir + lowDirectory);
          
          // only one should be able to create the lock file
          // the purpose of the lock file is to avoid a race
          // condition between the call to fs.exists() and
          // fs.mkdirs()... if only hadoop had a mkdir() function
          // that failed when the dir existed
          if (fs.createNewFile(lockPath)) {
            try {
              if (!fs.exists(lowDirectoryPath)) {
                if (fs.mkdirs(lowDirectoryPath))
                  return lowDirectory;
                log.warn("Failed to create " + lowDirectoryPath + " for unknown reason");
              } else {
                // name collision
                log.warn("Unlikely event occurred, name collision uuid = " + uuid + " endRow = " + endRow);
                uuid = UUID.randomUUID();
              }
            } finally {
              try {
                fs.delete(lockPath, false);
              } catch (IOException e) {
                log.warn("Failed to delete lock path " + lockPath + " " + e.getMessage());
              }
            }
          } else {
            log.warn("Unlikely event occurred, name collision uuid = " + uuid + " endRow = " + endRow);
            uuid = UUID.randomUUID();
          }
          
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
