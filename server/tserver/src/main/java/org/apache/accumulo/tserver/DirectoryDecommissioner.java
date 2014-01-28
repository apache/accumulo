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
package org.apache.accumulo.tserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashSet;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class contains utility code for switching a tablets default directory, if that default directory is no longer configured.
 */
public class DirectoryDecommissioner {

  private static final Logger log = Logger.getLogger(DirectoryDecommissioner.class);

  public static boolean isActiveVolume(Path dir) {
    for (String tableDir : ServerConstants.getTablesDirs()) {
      // use Path to normalize tableDir
      if (dir.toString().startsWith(new Path(tableDir).toString()))
        return true;
    }

    return false;
  }

  public static Path checkTabletDirectory(TabletServer tserver, VolumeManager vm, KeyExtent extent, Path dir) throws IOException {
    if (isActiveVolume(dir))
      return dir;

    if (!dir.getParent().getParent().getName().equals(ServerConstants.TABLE_DIR)) {
      throw new IllegalArgumentException("Unexpected table dir " + dir);
    }

    Path newDir = new Path(vm.choose(ServerConstants.getTablesDirs()) + "/" + dir.getParent().getName() + "/" + dir.getName());

    log.info("Updating directory for " + extent + " from " + dir + " to " + newDir);
    if (extent.isRootTablet()) {
      // the root tablet is special case, its files need to be copied if its dir is changed

      // this code needs to be idempotent

      FileSystem fs1 = vm.getFileSystemByPath(dir);
      FileSystem fs2 = vm.getFileSystemByPath(newDir);

      if (!same(fs1, dir, fs2, newDir)) {
        if (fs2.exists(newDir)) {
          Path newDirBackup = getBackupName(fs2, newDir);
          // never delete anything because were dealing with the root tablet
          // one reason this dir may exist is because this method failed previously
          log.info("renaming " + newDir + " to " + newDirBackup);
          if (!fs2.rename(newDir, newDirBackup)) {
            throw new IOException("Failed to rename " + newDir + " to " + newDirBackup);
          }
        }

        // do a lot of logging since this is the root tablet
        log.info("copying " + dir + " to " + newDir);
        if (!FileUtil.copy(fs1, dir, fs2, newDir, false, CachedConfiguration.getInstance())) {
          throw new IOException("Failed to copy " + dir + " to " + newDir);
        }

        // only set the new location in zookeeper after a successful copy
        log.info("setting root tablet location to " + newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());

        // rename the old dir to avoid confusion when someone looks at filesystem... its ok if we fail here and this does not happen because the location in
        // zookeeper is the authority
        Path dirBackup = getBackupName(fs1, dir);
        log.info("renaming " + dir + " to " + dirBackup);
        fs1.rename(dir, dirBackup);
      } else {
        log.info("setting root tablet location to " + newDir);
        MetadataTableUtil.setRootTabletDir(newDir.toString());
      }

      return dir;
    } else {
      MetadataTableUtil.updateTabletDir(extent, newDir.toString(), SystemCredentials.get(), tserver.getLock());
      return newDir;
    }
  }

  static boolean same(FileSystem fs1, Path dir, FileSystem fs2, Path newDir) throws FileNotFoundException, IOException {
    // its possible that a user changes config in such a way that two uris point to the same thing. Like hdfs://foo/a/b and hdfs://1.2.3.4/a/b both reference
    // the same thing because DNS resolves foo to 1.2.3.4. This method does not analyze uris to determine if equivalent, instead it inspects the contents of
    // what the uris point to.

    //this code is called infrequently and does not need to be optimized.  
    
    if (fs1.exists(dir) && fs2.exists(newDir)) {

      if (!fs1.isDirectory(dir))
        throw new IllegalArgumentException("expected " + dir + " to be a directory");


      if (!fs2.isDirectory(newDir))
        throw new IllegalArgumentException("expected " + newDir + " to be a directory");


      HashSet<String> names1 = getFileNames(fs1.listStatus(dir));
      HashSet<String> names2 = getFileNames(fs2.listStatus(newDir));

      if (names1.equals(names2)) {
        for (String name : names1)
          if (!hash(fs1, dir, name).equals(hash(fs2, newDir, name)))
            return false;
        return true;
      }

    }
    return false;
  }

  @SuppressWarnings("deprecation")
  private static HashSet<String> getFileNames(FileStatus[] filesStatuses) {
    HashSet<String> names = new HashSet<String>();
    for (FileStatus fileStatus : filesStatuses)
      if (fileStatus.isDir())
        throw new IllegalArgumentException("expected " + fileStatus.getPath() + " to be a file");
      else
        names.add(fileStatus.getPath().getName());
    return names;
  }

  private static String hash(FileSystem fs, Path dir, String name) throws IOException {
    FSDataInputStream in = fs.open(new Path(dir, name));
    try {
      return DigestUtils.sha1Hex(in);
    } finally {
      in.close();
    }

  }

  private static Path getBackupName(FileSystem fs, Path path) {
    SecureRandom rand = new SecureRandom();
    return new Path(path.getParent(), path.getName() + "_" + System.currentTimeMillis() + "_" + Math.abs(rand.nextInt()) + ".bak");
  }

}
