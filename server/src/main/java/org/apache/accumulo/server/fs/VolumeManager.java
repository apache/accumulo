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
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A wrapper around multiple hadoop FileSystem objects, which are assumed to be different volumes.
 * This also concentrates a bunch of meta-operations like waiting for SAFE_MODE, and closing WALs.
 */
public interface VolumeManager {
  
  // close the underlying FileSystems
  void close() throws IOException;
  
  // the mechanism by which the master ensures that tablet servers can no longer write to a WAL
  boolean closePossiblyOpenFile(Path path) throws IOException;
  
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
  FileSystem getFileSystemByPath(Path path);
  
  // get a mapping of volume to FileSystem
  Map<String, ? extends FileSystem> getFileSystems();
  
  // return the item in options that is in the same volume as source
  Path matchingFileSystem(Path source, String[] options);
  
  // create a new path in the same volume as the sourceDir
  String newPathOnSameVolume(String sourceDir, String suffix);
  
  // forward to the appropriate FileSystem object
  FileStatus[] listStatus(Path path) throws IOException;
  
  // forward to the appropriate FileSystem object
  boolean mkdirs(Path directory) throws IOException;
  
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
  
  // ambiguous references to files go here
  FileSystem getDefaultVolume();
  
  // forward to the appropriate FileSystem object
  FileStatus[] globStatus(Path path) throws IOException;

  // Convert a file or directory !METADATA reference into a path
  Path getFullPath(Key key);
  
  // Given a filename, figure out the qualified path given multiple namespaces
  Path getFullPath(String paths[], String fileName) throws IOException;

  // forward to the appropriate FileSystem object
  ContentSummary getContentSummary(Path dir) throws IOException;

  // decide on which of the given locations to create a new file
  String choose(String[] options);
}
