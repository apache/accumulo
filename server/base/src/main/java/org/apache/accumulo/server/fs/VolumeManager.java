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
package org.apache.accumulo.server.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around multiple hadoop FileSystem objects, which are assumed to be different volumes.
 * This also concentrates a bunch of meta-operations like waiting for SAFE_MODE, and closing WALs.
 * N.B. implementations must be thread safe.
 */
public interface VolumeManager extends AutoCloseable {

  enum FileType {
    TABLE(Constants.TABLE_DIR), WAL(Constants.WAL_DIR), RECOVERY(Constants.RECOVERY_DIR);

    private String dir;

    FileType(String dir) {
      this.dir = dir;
    }

    public String getDirectory() {
      return dir;
    }

    private static int endOfVolumeIndex(String path, String dir) {
      // Strip off the suffix that starts with the FileType (e.g. tables, wal, etc)
      int dirIndex = path.indexOf('/' + dir + '/');

      if (dirIndex != -1) {
        return dirIndex;
      }

      if (path.endsWith('/' + dir)) {
        return path.length() - (dir.length() + 1);
      }

      if (path.contains(":")) {
        throw new IllegalArgumentException(path + " is absolute, but does not contain " + dir);
      }
      return -1;
    }

    public Path getVolume(Path path) {
      String pathString = path.toString();

      int eopi = endOfVolumeIndex(pathString, dir);
      if (eopi != -1) {
        return new Path(pathString.substring(0, eopi + 1));
      }

      return null;
    }

    public Path removeVolume(Path path) {
      String pathString = path.toString();

      int eopi = endOfVolumeIndex(pathString, dir);
      if (eopi != -1) {
        return new Path(pathString.substring(eopi + 1));
      }

      return null;
    }
  }

  // close the underlying FileSystems
  @Override
  void close() throws IOException;

  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path dest) throws IOException;

  // forward to the appropriate FileSystem object's create method with overwrite flag set to true
  FSDataOutputStream overwrite(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1)
      throws IOException;

  // create a file, but only if it doesn't exist
  boolean createNewFile(Path writable) throws IOException;

  // create a file which can be sync'd to disk
  FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication, long blockSize)
      throws IOException;

  // delete a file
  boolean delete(Path path) throws IOException;

  // delete a directory and anything under it
  boolean deleteRecursively(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  boolean exists(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  FileStatus getFileStatus(Path path) throws IOException;

  // find the appropriate FileSystem object given a path
  FileSystem getFileSystemByPath(Path path);

  // return the item in options that is in the same file system as source
  Path matchingFileSystem(Path source, Set<String> options);

  // forward to appropriate FileSystem object. Does not support globbing.
  RemoteIterator<LocatedFileStatus> listFiles(final Path path, final boolean recursive)
      throws IOException;

  // forward to the appropriate FileSystem object
  FileStatus[] listStatus(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  boolean mkdirs(Path directory) throws IOException;

  // forward to the appropriate FileSystem object
  boolean mkdirs(Path path, FsPermission permission) throws IOException;

  // forward to the appropriate FileSystem object
  FSDataInputStream open(Path path) throws IOException;

  // forward to the appropriate FileSystem object, throws an exception if the paths are in different
  // volumes
  boolean rename(Path path, Path newPath) throws IOException;

  /**
   * Rename lots of files at once in a thread pool and return once all the threads have completed.
   * This operation should be idempotent to allow calling multiple times in the case of a partial
   * completion.
   */
  void bulkRename(Map<Path,Path> oldToNewPathMap, int poolSize, String poolName,
      String transactionId) throws IOException;

  // forward to the appropriate FileSystem object
  boolean moveToTrash(Path sourcePath) throws IOException;

  // forward to the appropriate FileSystem object
  short getDefaultReplication(Path logPath);

  // all volume are ready to provide service (not in SafeMode, for example)
  boolean isReady() throws IOException;

  // forward to the appropriate FileSystem object
  FileStatus[] globStatus(Path path) throws IOException;

  // decide on which of the given locations to create a new file
  String choose(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env, Set<String> options);

  // return all valid locations to create a new file
  Set<String> choosable(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
      Set<String> options);

  // are sync and flush supported for the given path
  boolean canSyncAndFlush(Path path);

  /**
   * Fetch the first configured instance Volume
   */
  default Volume getFirst() {
    return getVolumes().iterator().next();
  }

  /**
   * Fetch the configured instance Volumes
   */
  Collection<Volume> getVolumes();

  Logger log = LoggerFactory.getLogger(VolumeManager.class);

  static InstanceId getInstanceIDFromHdfs(Path instanceDirectory, Configuration hadoopConf) {
    try {
      FileSystem fs =
          VolumeConfiguration.fileSystemForPath(instanceDirectory.toString(), hadoopConf);
      FileStatus[] files = null;
      try {
        files = fs.listStatus(instanceDirectory);
      } catch (FileNotFoundException ex) {
        // ignored
      }
      log.debug("Trying to read instance id from {}", instanceDirectory);
      if (files == null || files.length == 0) {
        log.error("unable to obtain instance id at {}", instanceDirectory);
        throw new RuntimeException(
            "Accumulo not initialized, there is no instance id at " + instanceDirectory);
      } else if (files.length != 1) {
        log.error("multiple potential instances in {}", instanceDirectory);
        throw new RuntimeException(
            "Accumulo found multiple possible instance ids in " + instanceDirectory);
      } else {
        return InstanceId.of(files[0].getPath().getName());
      }
    } catch (IOException e) {
      log.error("Problem reading instance id out of hdfs at " + instanceDirectory, e);
      throw new RuntimeException(
          "Can't tell if Accumulo is initialized; can't read instance id at " + instanceDirectory,
          e);
    } catch (IllegalArgumentException exception) {
      /* HDFS throws this when there's a UnknownHostException due to DNS troubles. */
      if (exception.getCause() instanceof UnknownHostException) {
        log.error("Problem reading instance id out of hdfs at " + instanceDirectory, exception);
      }
      throw exception;
    }
  }

}
