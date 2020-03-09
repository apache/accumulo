/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.accumulo.server.fs.VolumeChooser.VolumeChooserException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class VolumeManagerImpl implements VolumeManager {

  private static final Logger log = LoggerFactory.getLogger(VolumeManagerImpl.class);

  private static final HashSet<String> WARNED_ABOUT_SYNCONCLOSE = new HashSet<>();

  private final Map<String,Volume> volumesByName;
  private final Multimap<URI,Volume> volumesByFileSystemUri;
  private final Volume defaultVolume;
  private final VolumeChooser chooser;
  private final Configuration hadoopConf;

  protected VolumeManagerImpl(Map<String,Volume> volumes, Volume defaultVolume,
      AccumuloConfiguration conf, Configuration hadoopConf) {
    this.volumesByName = volumes;
    this.defaultVolume = defaultVolume;
    // We may have multiple directories used in a single FileSystem (e.g. testing)
    this.volumesByFileSystemUri = invertVolumesByFileSystem(volumesByName);
    ensureSyncIsEnabled();
    // if they supplied a property and we cannot load it, then fail hard
    VolumeChooser chooser1;
    try {
      chooser1 = Property.createInstanceFromPropertyName(conf, Property.GENERAL_VOLUME_CHOOSER,
          VolumeChooser.class, null);
    } catch (NullPointerException npe) {
      chooser1 = null;
      // null chooser handled below
    }
    if (chooser1 == null) {
      throw new VolumeChooserException(
          "Failed to load volume chooser specified by " + Property.GENERAL_VOLUME_CHOOSER);
    }
    chooser = chooser1;
    this.hadoopConf = hadoopConf;
  }

  private Multimap<URI,Volume> invertVolumesByFileSystem(Map<String,Volume> forward) {
    Multimap<URI,Volume> inverted = HashMultimap.create();
    forward.values().forEach(volume -> inverted.put(volume.getFileSystem().getUri(), volume));
    return inverted;
  }

  // for testing only
  public static VolumeManager getLocalForTesting(String localBasePath) throws IOException {
    AccumuloConfiguration accConf = DefaultConfiguration.getInstance();
    Configuration hadoopConf = new Configuration();
    Volume defaultLocalVolume = new VolumeImpl(FileSystem.getLocal(hadoopConf), localBasePath);
    return new VolumeManagerImpl(Collections.singletonMap("", defaultLocalVolume),
        defaultLocalVolume, accConf, hadoopConf);
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    for (Volume volume : volumesByName.values()) {
      try {
        volume.getFileSystem().close();
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public FSDataOutputStream create(Path path) throws IOException {
    return getFileSystemByPath(path).create(path);
  }

  @Override
  public FSDataOutputStream overwrite(Path path) throws IOException {
    return getFileSystemByPath(path).create(path, true);
  }

  private static long correctBlockSize(Configuration conf, long blockSize) {
    if (blockSize <= 0)
      blockSize = conf.getLong("dfs.block.size", 67108864); // 64MB default
    int checkSum = conf.getInt("io.bytes.per.checksum", 512);
    blockSize -= blockSize % checkSum;
    return Math.max(blockSize, checkSum);
  }

  private static int correctBufferSize(Configuration conf, int bufferSize) {
    return bufferSize <= 0 ? conf.getInt("io.file.buffer.size", 4096) : bufferSize;
  }

  @Override
  public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication,
      long blockSize) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    blockSize = correctBlockSize(fs.getConf(), blockSize);
    bufferSize = correctBufferSize(fs.getConf(), bufferSize);
    return fs.create(path, overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public boolean createNewFile(Path path) throws IOException {
    return getFileSystemByPath(path).createNewFile(path);
  }

  @Override
  public FSDataOutputStream createSyncable(Path logPath, int bufferSize, short replication,
      long blockSize) throws IOException {
    FileSystem fs = getFileSystemByPath(logPath);
    blockSize = correctBlockSize(fs.getConf(), blockSize);
    bufferSize = correctBufferSize(fs.getConf(), bufferSize);
    EnumSet<CreateFlag> set = EnumSet.of(CreateFlag.SYNC_BLOCK, CreateFlag.CREATE);
    log.debug("creating {} with CreateFlag set: {}", logPath, set);
    try {
      return fs.create(logPath, FsPermission.getDefault(), set, bufferSize, replication, blockSize,
          null);
    } catch (Exception ex) {
      log.debug("Exception", ex);
      return fs.create(logPath, true, bufferSize, replication, blockSize);
    }
  }

  @Override
  public boolean delete(Path path) throws IOException {
    return getFileSystemByPath(path).delete(path, false);
  }

  @Override
  public boolean deleteRecursively(Path path) throws IOException {
    return getFileSystemByPath(path).delete(path, true);
  }

  protected void ensureSyncIsEnabled() {
    for (Entry<String,Volume> entry : volumesByName.entrySet()) {
      FileSystem fs = entry.getValue().getFileSystem();

      if (fs instanceof DistributedFileSystem) {
        // Avoid use of DFSConfigKeys since it's private
        final String DFS_SUPPORT_APPEND = "dfs.support.append";
        final String DFS_DATANODE_SYNCONCLOSE = "dfs.datanode.synconclose";
        final String ticketMessage = "See ACCUMULO-623 and ACCUMULO-1637 for more details.";

        // If either of these parameters are configured to be false, fail.
        // This is a sign that someone is writing bad configuration.
        if (!fs.getConf().getBoolean(DFS_SUPPORT_APPEND, true)) {
          String msg = "Accumulo requires that " + DFS_SUPPORT_APPEND
              + " not be configured as false. " + ticketMessage;
          // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
          log.error("FATAL {}", msg);
          throw new RuntimeException(msg);
        }

        // Warn if synconclose isn't set
        if (!fs.getConf().getBoolean(DFS_DATANODE_SYNCONCLOSE, false)) {
          // Only warn once per process per volume URI
          synchronized (WARNED_ABOUT_SYNCONCLOSE) {
            if (!WARNED_ABOUT_SYNCONCLOSE.contains(entry.getKey())) {
              WARNED_ABOUT_SYNCONCLOSE.add(entry.getKey());
              log.warn("{} set to false in hdfs-site.xml: data loss is possible"
                  + " on hard system reset or power loss", DFS_DATANODE_SYNCONCLOSE);
            }
          }
        }
      }
    }
  }

  @Override
  public boolean exists(Path path) throws IOException {
    return getFileSystemByPath(path).exists(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return getFileSystemByPath(path).getFileStatus(path);
  }

  @Override
  public FileSystem getFileSystemByPath(Path path) {
    if (!requireNonNull(path).toString().contains(":")) {
      return defaultVolume.getFileSystem();
    }
    FileSystem desiredFs;
    try {
      desiredFs = path.getFileSystem(hadoopConf);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    URI desiredFsUri = desiredFs.getUri();
    Collection<Volume> candidateVolumes = volumesByFileSystemUri.get(desiredFsUri);
    if (candidateVolumes != null) {
      return candidateVolumes.stream().filter(volume -> volume.isValidPath(path))
          .map(Volume::getFileSystem).findFirst().orElse(desiredFs);
    } else {
      log.debug("Could not determine volume for Path: {}", path);
      return desiredFs;
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    return getFileSystemByPath(path).listStatus(path);
  }

  @Override
  public boolean mkdirs(Path path) throws IOException {
    return getFileSystemByPath(path).mkdirs(path);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    return getFileSystemByPath(path).mkdirs(path, permission);
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return getFileSystemByPath(path).open(path);
  }

  @Override
  public boolean rename(Path path, Path newPath) throws IOException {
    FileSystem source = getFileSystemByPath(path);
    FileSystem dest = getFileSystemByPath(newPath);
    if (source != dest) {
      throw new UnsupportedOperationException(
          "Cannot rename files across volumes: " + path + " -> " + newPath);
    }
    return source.rename(path, newPath);
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    Trash trash = new Trash(fs, fs.getConf());
    return trash.moveToTrash(path);
  }

  @Override
  public short getDefaultReplication(Path path) {
    return getFileSystemByPath(path).getDefaultReplication(path);
  }

  public static VolumeManager get(AccumuloConfiguration conf, final Configuration hadoopConf)
      throws IOException {
    final Map<String,Volume> volumes = new HashMap<>();

    // The "default" Volume for Accumulo (in case no volumes are specified)
    for (String volumeUriOrDir : VolumeConfiguration.getVolumeUris(conf, hadoopConf)) {
      if (volumeUriOrDir.isBlank())
        throw new IllegalArgumentException("Empty volume specified in configuration");

      if (volumeUriOrDir.startsWith("viewfs"))
        throw new IllegalArgumentException("Cannot use viewfs as a volume");

      // We require a URI here, fail if it doesn't look like one
      if (volumeUriOrDir.contains(":")) {
        volumes.put(volumeUriOrDir, new VolumeImpl(new Path(volumeUriOrDir), hadoopConf));
      } else {
        throw new IllegalArgumentException("Expected fully qualified URI for "
            + Property.INSTANCE_VOLUMES.getKey() + " got " + volumeUriOrDir);
      }
    }

    Volume defaultVolume = VolumeConfiguration.getDefaultVolume(hadoopConf, conf);
    return new VolumeManagerImpl(volumes, defaultVolume, conf, hadoopConf);
  }

  @Override
  public boolean isReady() throws IOException {
    for (Volume volume : volumesByName.values()) {
      final FileSystem fs = volume.getFileSystem();
      if (!(fs instanceof DistributedFileSystem))
        continue;
      final DistributedFileSystem dfs = (DistributedFileSystem) fs;
      // Returns true when safemode is on
      if (dfs.setSafeMode(SafeModeAction.SAFEMODE_GET)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return getFileSystemByPath(pathPattern).globStatus(pathPattern);
  }

  @Override
  public Path matchingFileSystem(Path source, Set<String> options) {
    URI sourceUri = source.toUri();
    return options.stream().filter(opt -> {
      URI optUri = URI.create(opt);
      return sourceUri.getScheme().equals(optUri.getScheme())
          && Objects.equals(sourceUri.getAuthority(), optUri.getAuthority());
    }).map((String opt) -> new Path(opt)).findFirst().orElse(null);
  }

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    final String choice;
    choice = chooser.choose(env, options);
    if (!options.contains(choice)) {
      String msg = "The configured volume chooser, '" + chooser.getClass()
          + "', or one of its delegates returned a volume not in the set of options provided";
      throw new VolumeChooserException(msg);
    }
    return choice;
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options) {
    final Set<String> choices = chooser.choosable(env, options);
    for (String choice : choices) {
      if (!options.contains(choice)) {
        String msg = "The configured volume chooser, '" + chooser.getClass()
            + "', or one of its delegates returned a volume not in the set of options provided";
        throw new VolumeChooserException(msg);
      }
    }
    return choices;
  }

  @Override
  public boolean canSyncAndFlush(Path path) {
    // the assumption is all filesystems support sync/flush except
    // for HDFS erasure coding. not checking hdfs config options
    // since that's already checked in ensureSyncIsEnabled()
    FileSystem fs = getFileSystemByPath(path);
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        ErasureCodingPolicy currEC = dfs.getErasureCodingPolicy(path);
        if (currEC != null && !currEC.isReplicationPolicy()) {
          return false;
        }
      } catch (IOException e) {
        // don't spam warnings...if dir doesn't exist or not EC
        // we don't really care if the above failed
        log.debug("exception getting EC policy for " + path, e);
      }
    }
    return true;
  }

  @Override
  public Volume getDefaultVolume() {
    return defaultVolume;
  }

  @Override
  public Collection<Volume> getVolumes() {
    return volumesByName.values();
  }
}
