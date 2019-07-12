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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.volume.NonConfiguredVolume;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeChooser.VolumeChooserException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
    this.volumesByFileSystemUri = HashMultimap.create();
    invertVolumesByFileSystem(volumesByName, volumesByFileSystemUri);
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

  private void invertVolumesByFileSystem(Map<String,Volume> forward,
      Multimap<URI,Volume> inverted) {
    for (Volume volume : forward.values()) {
      inverted.put(volume.getFileSystem().getUri(), volume);
    }
  }

  public static org.apache.accumulo.server.fs.VolumeManager getLocal(String localBasePath)
      throws IOException {
    AccumuloConfiguration accConf = DefaultConfiguration.getInstance();
    Configuration hadoopConf = new Configuration();
    Volume defaultLocalVolume =
        VolumeConfiguration.create(FileSystem.getLocal(hadoopConf), localBasePath);

    // The default volume gets placed in the map, but local filesystem is only used for testing
    // purposes
    return new VolumeManagerImpl(Collections.singletonMap(DEFAULT, defaultLocalVolume),
        defaultLocalVolume, accConf, hadoopConf);
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    for (Volume volume : volumesByName.values()) {
      try {
        volume.getFileSystem().close();
      } catch (IOException e) {
        ex = e;
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public FSDataOutputStream create(Path path) throws IOException {
    requireNonNull(path);

    Volume v = getVolumeByPath(path);

    return v.getFileSystem().create(path);
  }

  @Override
  public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
    requireNonNull(path);

    Volume v = getVolumeByPath(path);

    return v.getFileSystem().create(path, overwrite);
  }

  private static long correctBlockSize(Configuration conf, long blockSize) {
    if (blockSize <= 0)
      blockSize = conf.getLong("dfs.block.size", 67108864);

    int checkSum = conf.getInt("io.bytes.per.checksum", 512);
    blockSize -= blockSize % checkSum;
    blockSize = Math.max(blockSize, checkSum);
    return blockSize;
  }

  private static int correctBufferSize(Configuration conf, int bufferSize) {
    if (bufferSize <= 0)
      bufferSize = conf.getInt("io.file.buffer.size", 4096);
    return bufferSize;
  }

  @Override
  public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication,
      long blockSize) throws IOException {
    requireNonNull(path);

    Volume v = getVolumeByPath(path);
    FileSystem fs = v.getFileSystem();
    blockSize = correctBlockSize(fs.getConf(), blockSize);
    bufferSize = correctBufferSize(fs.getConf(), bufferSize);
    return fs.create(path, overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public boolean createNewFile(Path path) throws IOException {
    requireNonNull(path);

    Volume v = getVolumeByPath(path);
    return v.getFileSystem().createNewFile(path);
  }

  @Override
  public FSDataOutputStream createSyncable(Path logPath, int bufferSize, short replication,
      long blockSize) throws IOException {
    Volume v = getVolumeByPath(logPath);
    FileSystem fs = v.getFileSystem();
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
    return getVolumeByPath(path).getFileSystem().delete(path, false);
  }

  @Override
  public boolean deleteRecursively(Path path) throws IOException {
    return getVolumeByPath(path).getFileSystem().delete(path, true);
  }

  protected void ensureSyncIsEnabled() {
    for (Entry<String,Volume> entry : getFileSystems().entrySet()) {
      FileSystem fs = entry.getValue().getFileSystem();

      if (fs instanceof DistributedFileSystem) {
        // Avoid use of DFSConfigKeys since it's private
        final String DFS_SUPPORT_APPEND = "dfs.support.append",
            DFS_DATANODE_SYNCONCLOSE = "dfs.datanode.synconclose";
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
    return getVolumeByPath(path).getFileSystem().exists(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return getVolumeByPath(path).getFileSystem().getFileStatus(path);
  }

  @Override
  public Volume getVolumeByPath(Path path) {
    if (path.toString().contains(":")) {
      try {
        FileSystem desiredFs = path.getFileSystem(hadoopConf);
        URI desiredFsUri = desiredFs.getUri();
        Collection<Volume> candidateVolumes = volumesByFileSystemUri.get(desiredFsUri);
        if (candidateVolumes != null) {
          for (Volume candidateVolume : candidateVolumes) {
            if (candidateVolume.isValidPath(path)) {
              return candidateVolume;
            }
          }
        } else {
          log.debug("Could not determine volume for Path: {}", path);
        }

        return new NonConfiguredVolume(desiredFs);
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    return defaultVolume;
  }

  private Map<String,Volume> getFileSystems() {
    return volumesByName;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    return getVolumeByPath(path).getFileSystem().listStatus(path);
  }

  @Override
  public boolean mkdirs(Path path) throws IOException {
    return getVolumeByPath(path).getFileSystem().mkdirs(path);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    return getVolumeByPath(path).getFileSystem().mkdirs(path, permission);
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return getVolumeByPath(path).getFileSystem().open(path);
  }

  @Override
  public boolean rename(Path path, Path newPath) throws IOException {
    Volume srcVolume = getVolumeByPath(path);
    Volume destVolume = getVolumeByPath(newPath);
    FileSystem source = srcVolume.getFileSystem();
    FileSystem dest = destVolume.getFileSystem();
    if (source != dest) {
      throw new UnsupportedOperationException(
          "Cannot rename files across volumes: " + path + " -> " + newPath);
    }
    return source.rename(path, newPath);
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    FileSystem fs = getVolumeByPath(path).getFileSystem();
    Trash trash = new Trash(fs, fs.getConf());
    return trash.moveToTrash(path);
  }

  @Override
  public short getDefaultReplication(Path path) {
    Volume v = getVolumeByPath(path);
    return v.getFileSystem().getDefaultReplication(path);
  }

  private static final String DEFAULT = "";

  public static VolumeManager get(AccumuloConfiguration conf, final Configuration hadoopConf)
      throws IOException {
    final Map<String,Volume> volumes = new HashMap<>();

    // The "default" Volume for Accumulo (in case no volumes are specified)
    for (String volumeUriOrDir : VolumeConfiguration.getVolumeUris(conf, hadoopConf)) {
      if (volumeUriOrDir.equals(DEFAULT))
        throw new IllegalArgumentException("Cannot re-define the default volume");

      if (volumeUriOrDir.startsWith("viewfs"))
        throw new IllegalArgumentException("Cannot use viewfs as a volume");

      // We require a URI here, fail if it doesn't look like one
      if (volumeUriOrDir.contains(":")) {
        volumes.put(volumeUriOrDir,
            VolumeConfiguration.create(new Path(volumeUriOrDir), hadoopConf));
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
    for (Volume volume : getFileSystems().values()) {
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
    return getVolumeByPath(pathPattern).getFileSystem().globStatus(pathPattern);
  }

  @Override
  public Path getFullPath(Key key) {
    // TODO sanity check col fam
    String relPath = key.getColumnQualifierData().toString();
    byte[] tableId = KeyExtent.tableOfMetadataRow(key.getRow());
    return getFullPath(TableId.of(new String(tableId)), relPath);
  }

  @Override
  public Path matchingFileSystem(Path source, String[] options) {
    try {
      if (ViewFSUtils.isViewFS(source, hadoopConf)) {
        return ViewFSUtils.matchingFileSystem(source, options, hadoopConf);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    URI uri1 = source.toUri();
    for (String option : options) {
      URI uri3 = URI.create(option);
      if (uri1.getScheme().equals(uri3.getScheme())) {
        String a1 = uri1.getAuthority();
        String a2 = uri3.getAuthority();
        if ((a1 == null && a2 == null) || (a1 != null && a1.equals(a2)))
          return new Path(option);
      }
    }
    return null;
  }

  @Override
  public Path getFullPath(TableId tableId, String path) {
    if (path.contains(":"))
      return new Path(path);

    if (path.startsWith("../"))
      path = path.substring(2);
    else if (path.startsWith("/"))
      path = "/" + tableId.canonical() + path;
    else
      throw new IllegalArgumentException("Unexpected path prefix " + path);

    return getFullPath(FileType.TABLE, path);
  }

  private static final String RFILE_SUFFIX = "." + RFile.EXTENSION;

  @Override
  public Path getFullPath(FileType fileType, String path) {
    int colon = path.indexOf(':');
    if (colon > -1) {
      // Check if this is really an absolute path or if this is a 1.4 style relative path for a WAL
      if (fileType == FileType.WAL && path.charAt(colon + 1) != '/') {
        path = path.substring(path.indexOf('/'));
      } else {
        return new Path(path);
      }
    }

    if (path.startsWith("/"))
      path = path.substring(1);

    // ACCUMULO-2974 To ensure that a proper absolute path is created, the caller needs to include
    // the table ID
    // in the relative path. Fail when this doesn't appear to happen.
    if (fileType == FileType.TABLE) {
      // Trailing slash doesn't create an additional element
      String[] pathComponents = StringUtils.split(path, Path.SEPARATOR_CHAR);

      // Is an rfile
      if (path.endsWith(RFILE_SUFFIX)) {
        if (pathComponents.length < 3) {
          throw new IllegalArgumentException("Fewer components in file path than expected");
        }
      } else {
        // is a directory
        if (pathComponents.length < 2) {
          throw new IllegalArgumentException("Fewer components in directory path than expected");
        }
      }
    }

    // normalize the path
    Path fullPath = new Path(defaultVolume.getBasePath(), fileType.getDirectory());
    fullPath = new Path(fullPath, path);

    FileSystem fs = getVolumeByPath(fullPath).getFileSystem();
    return fs.makeQualified(fullPath);
  }

  @Override
  public ContentSummary getContentSummary(Path dir) throws IOException {
    return getVolumeByPath(dir).getFileSystem().getContentSummary(dir);
  }

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) {
    final String choice;
    choice = chooser.choose(env, options);

    if (!(ArrayUtils.contains(options, choice))) {
      String msg = "The configured volume chooser, '" + chooser.getClass()
          + "', or one of its delegates returned a volume not in the set of options provided";
      throw new VolumeChooserException(msg);
    }

    return choice;
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
