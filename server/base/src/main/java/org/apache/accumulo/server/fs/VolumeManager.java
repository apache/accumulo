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
package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A wrapper around multiple hadoop FileSystem objects, which are assumed to be different volumes. This also concentrates a bunch of meta-operations like
 * waiting for SAFE_MODE, and closing WALs. N.B. implementations must be thread safe.
 */
public interface VolumeManager {

  public static enum FileType {
    TABLE(ServerConstants.TABLE_DIR), WAL(ServerConstants.WAL_DIR), RECOVERY(ServerConstants.RECOVERY_DIR);

    private String dir;

    FileType(String dir) {
      this.dir = dir;
    }

    public String getDirectory() {
      return dir;
    }

    private static int endOfVolumeIndex(String path, String dir) {
      // Strip off the suffix that starts with the FileType (e.g. tables, wal, etc)
      int dirIndex = path.indexOf('/' + dir);
      if (dirIndex != -1) {
        return dirIndex;
      }

      if (path.contains(":"))
        throw new IllegalArgumentException(path + " is absolute, but does not contain " + dir);
      return -1;

    }

    public Path getVolume(Path path) {
      String pathString = path.toString();

      int eopi = endOfVolumeIndex(pathString, dir);
      if (eopi != -1)
        return new Path(pathString.substring(0, eopi + 1));

      return null;
    }

    public Path removeVolume(Path path) {
      String pathString = path.toString();

      int eopi = endOfVolumeIndex(pathString, dir);
      if (eopi != -1)
        return new Path(pathString.substring(eopi + 1));

      return null;
    }
  }

  // close the underlying FileSystems
  void close() throws IOException;

  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path dest) throws IOException;

  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path path, boolean b) throws IOException;

  // forward to the appropriate FileSystem object
  FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1) throws IOException;

  // create a file, but only if it doesn't exist
  boolean createNewFile(Path writable) throws IOException;

  // create a file which can be sync'd to disk
  FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication, long blockSize) throws IOException;

  // delete a file
  boolean delete(Path path) throws IOException;

  // delete a directory and anything under it
  boolean deleteRecursively(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  boolean exists(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  FileStatus getFileStatus(Path path) throws IOException;

  // find the appropriate FileSystem object given a path
  Volume getVolumeByPath(Path path);

  // return the item in options that is in the same volume as source
  Path matchingFileSystem(Path source, String[] options);

  // forward to the appropriate FileSystem object
  FileStatus[] listStatus(Path path) throws IOException;

  // forward to the appropriate FileSystem object
  boolean mkdirs(Path directory) throws IOException;

  // forward to the appropriate FileSystem object
  boolean mkdirs(Path path, FsPermission permission) throws IOException;

  // forward to the appropriate FileSystem object
  FSDataInputStream open(Path path) throws IOException;

  // forward to the appropriate FileSystem object, throws an exception if the paths are in different volumes
  boolean rename(Path path, Path newPath) throws IOException;

  // forward to the appropriate FileSystem object
  boolean moveToTrash(Path sourcePath) throws IOException;

  // forward to the appropriate FileSystem object
  short getDefaultReplication(Path logPath);

  // forward to the appropriate FileSystem object
  boolean isFile(Path path) throws IOException;

  // all volume are ready to provide service (not in SafeMode, for example)
  boolean isReady() throws IOException;

  // forward to the appropriate FileSystem object
  FileStatus[] globStatus(Path path) throws IOException;

  // Convert a file or directory metadata reference into a path
  Path getFullPath(Key key);

  Path getFullPath(Table.ID tableId, String path);

  // Given a filename, figure out the qualified path given multiple namespaces
  Path getFullPath(FileType fileType, String fileName) throws IOException;

  // forward to the appropriate FileSystem object
  ContentSummary getContentSummary(Path dir) throws IOException;

  // decide on which of the given locations to create a new file
  String choose(VolumeChooserEnvironment env, String[] options);

  /**
   * Fetch the default Volume
   */
  public Volume getDefaultVolume();

  /**
   * Fetch the configured Volumes, excluding the default Volume
   */
  public Collection<Volume> getVolumes();

}
