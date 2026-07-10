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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.spi.fs.VolumeChooser;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;

public class VolumeManagerImpl implements VolumeManager {

  private static final Logger log = LoggerFactory.getLogger(VolumeManagerImpl.class);

  private static final HashSet<String> WARNED_ABOUT_SYNCONCLOSE = new HashSet<>();

  private static final Cache<Pair<Configuration,String>,Configuration> HDFS_CONFIGS_FOR_VOLUME =
      Caches.getInstance().createNewBuilder(CacheName.VOLUME_HDFS_CONFIGS, false)
          .expireAfterWrite(24, TimeUnit.HOURS).build();

  private final Map<String,Volume> volumesByName;
  private final Multimap<URI,Volume> volumesByFileSystemUri;
  private final Map<FileSystem,Volume> bulkDeleteFileSystems;
  private final VolumeChooser chooser;
  private final AccumuloConfiguration conf;
  private final Configuration hadoopConf;

  protected VolumeManagerImpl(Map<String,Volume> volumes, AccumuloConfiguration conf,
      Configuration hadoopConf) {
    this.volumesByName = volumes;
    // We may have multiple directories used in a single FileSystem (e.g. testing)
    this.volumesByFileSystemUri = invertVolumesByFileSystem(volumesByName);

    this.bulkDeleteFileSystems = new HashMap<>();
    for (Volume v : volumes.values()) {
      FileSystem fs = v.getFileSystem();
      String base = v.getBasePath().isBlank() ? "/" : v.getBasePath();
      Path basePath = fs.makeQualified(new Path(base));
      try {
        if (fs.hasPathCapability(basePath, "fs.capability.bulk.delete")) {
          try (BulkDelete bulk = fs.createBulkDelete(basePath)) {
            // Don't use the BulkDelete API if the page size is only 1.
            // The DefaultBulkDeleteOperation implementation has a page size of 1.
            // The S3A FileSystem implementation has a default of 250 and
            // is configured by the property `fs.s3a.bulk.delete.page.size`.
            if (bulk.pageSize() > 1) {
              this.bulkDeleteFileSystems.put(fs, v);
            }
          }
        }
      } catch (IllegalArgumentException | IOException e) {
        log.warn("Error determining bulk delete capability for volume {}", v.getBasePath(), e);
      }
    }

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
      throw new IllegalStateException(
          "Failed to load volume chooser specified by " + Property.GENERAL_VOLUME_CHOOSER);
    }
    chooser = chooser1;
    this.conf = conf;
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
    FileSystem localFS = FileSystem.getLocal(hadoopConf);
    Volume defaultLocalVolume = new VolumeImpl(localFS, localBasePath);
    return new VolumeManagerImpl(Collections.singletonMap("", defaultLocalVolume), accConf,
        hadoopConf);
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
    if (blockSize <= 0) {
      blockSize = conf.getLong("dfs.block.size", 67108864); // 64MB default
    }
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

  @Override
  public Map<Path,DeleteStatus> deleteBulk(Collection<Path> paths) throws IOException {

    requireNonNull(paths);

    if (paths.isEmpty()) {
      return Map.of();
    }

    if (paths.size() == 1) {
      Path p = paths.iterator().next();
      try {
        if (delete(p)) {
          return Map.of(p, DeleteStatus.TRUE);
        } else {
          return Map.of(p, DeleteStatus.FALSE);
        }
      } catch (IOException e) {
        return Map.of(p, DeleteStatus.ERROR);
      }
    }

    final Map<Volume,ArrayList<Path>> pathsSupportingBulkDelete = new HashMap<>();
    final List<Path> pathsNotSupportingBulkDelete = new ArrayList<>();
    final Map<Path,DeleteStatus> results = new ConcurrentHashMap<>();
    final List<Future<Void>> futures = new LinkedList<>();

    for (Path p : paths) {
      FileSystem fs = getFileSystemByPath(p);
      Volume v = this.bulkDeleteFileSystems.get(fs);
      if (v != null) {
        pathsSupportingBulkDelete.computeIfAbsent(v, (k) -> new ArrayList<Path>()).add(p);
      } else {
        pathsNotSupportingBulkDelete.add(p);
      }
    }

    if (!pathsNotSupportingBulkDelete.isEmpty()) {
      final ExecutorService delSvc;
      if (pathsNotSupportingBulkDelete.size() < 4) {
        // Do not bother creating a thread pool and threads for a few files.
        delSvc = MoreExecutors.newDirectExecutorService();
      } else {
        delSvc = Executors.newFixedThreadPool(8);
      }
      pathsNotSupportingBulkDelete.forEach(p -> {
        futures.add(delSvc.submit(() -> {
          try {
            if (delete(p)) {
              results.put(p, DeleteStatus.TRUE);
            } else {
              results.put(p, DeleteStatus.FALSE);
            }
            return null;
          } catch (IOException e) {
            log.error("Error deleting file at {}", p, e);
            results.put(p, DeleteStatus.ERROR);
            return null;
          }
        }));
      });
      delSvc.shutdown();
    }

    for (Entry<Volume,ArrayList<Path>> e : pathsSupportingBulkDelete.entrySet()) {
      Volume v = e.getKey();
      List<Path> deletes = e.getValue();
      FileSystem fs = v.getFileSystem();
      Path basePath = fs.makeQualified(new Path(v.getBasePath()));
      try (BulkDelete bulk = fs.createBulkDelete(basePath)) {
        int batchSize = bulk.pageSize();
        for (int i = 0; i <= deletes.size(); i += batchSize) {
          List<Path> subset = deletes.subList(i, Math.min(i + batchSize, deletes.size()));
          List<Entry<Path,String>> errors = bulk.bulkDelete(subset);
          errors.forEach((entry) -> {
            log.error("Failed to delete file at {}, reason: {}", entry.getKey(), entry.getValue());
            results.put(entry.getKey(), DeleteStatus.ERROR);
            if (!subset.remove(entry.getKey())) {
              log.error("Did not find error path {} in input set {}", entry.getKey(), subset);
            }
          });
          subset.forEach(success -> results.put(success, DeleteStatus.TRUE));
        }
      }
    }

    while (!futures.isEmpty()) {
      Iterator<Future<Void>> iter = futures.iterator();
      while (iter.hasNext()) {
        Future<Void> f = iter.next();
        if (f.isDone()) {
          iter.remove();
        }
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
    return results;
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
          throw new IllegalStateException(msg);
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
    FileSystem desiredFs;
    try {
      Configuration volumeConfig = hadoopConf;
      for (String vol : volumesByName.keySet()) {
        if (path.toString().startsWith(vol)) {
          volumeConfig = getVolumeManagerConfiguration(conf, hadoopConf, vol);
          break;
        }
      }
      desiredFs = requireNonNull(path).getFileSystem(volumeConfig);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    URI desiredFsUri = desiredFs.getUri();
    Collection<Volume> candidateVolumes = volumesByFileSystemUri.get(desiredFsUri);
    if (candidateVolumes != null) {
      return candidateVolumes.stream().filter(volume -> volume.containsPath(path))
          .map(Volume::getFileSystem).findFirst().orElse(desiredFs);
    } else {
      log.debug("Could not determine volume for Path: {}", path);
      return desiredFs;
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(final Path path, final boolean recursive)
      throws IOException {
    return getFileSystemByPath(path).listFiles(path, recursive);
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
  public void bulkRename(Map<Path,Path> oldToNewPathMap, ExecutorService workerPool, FateId fateId)
      throws IOException {
    List<Future<Void>> results = new ArrayList<>(oldToNewPathMap.size());
    oldToNewPathMap.forEach((oldPath, newPath) -> results.add(workerPool.submit(() -> {
      boolean success;
      try {
        success = rename(oldPath, newPath);
      } catch (IOException e) {
        // The rename could have failed because this is the second time its running (failures
        // could cause this to run multiple times).
        if (!exists(newPath) || exists(oldPath)) {
          throw e;
        }
        log.debug(
            "Ignoring rename exception for {} because destination already exists. orig: {} new: {}",
            fateId, oldPath, newPath, e);
        success = true;
      }
      if (!success && (!exists(newPath) || exists(oldPath))) {
        throw new IOException("Rename operation " + fateId + " returned false. orig: " + oldPath
            + " new: " + newPath);
      } else if (log.isTraceEnabled()) {
        log.trace("{} moved {} to {}", fateId, oldPath, newPath);
      }
      return null;
    })));

    for (Future<Void> future : results) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean moveToTrash(Path path) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    String key = CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
    log.trace("{}: {}", key, fs.getConf().get(key));
    Trash trash = new Trash(fs, fs.getConf());
    log.trace("Hadoop Trash is enabled for {}: {}", path, trash.isEnabled());
    return trash.moveToTrash(path);
  }

  @Override
  public short getDefaultReplication(Path path) {
    return getFileSystemByPath(path).getDefaultReplication(path);
  }

  /**
   * The Hadoop Configuration object does not currently allow for duplicate properties to be set in
   * a single Configuration for different FileSystem URIs. Here we will look for properties in the
   * Accumulo configuration of the form:
   *
   * <pre>
   * instance.volume.config.&lt;volume-uri&gt;.&lt;hdfs-property&gt;
   * </pre>
   *
   * We will use these properties to return a new Configuration object that can be used with the
   * FileSystem URI to override properties in the original Configuration. If these properties are
   * not set for a volume, then the original Configuration is returned. If they are set, a new
   * Configuration is created with the overridden properties set. In either case, the returned
   * Configuration is cached, to avoid unnecessary recomputation. This works because these override
   * properties are instance properties and cannot change while the system is running.
   *
   * @param conf AccumuloConfiguration object
   * @param hadoopConf Hadoop Configuration object
   * @param filesystemURI Volume Filesystem URI
   * @return Hadoop Configuration with custom overrides for this FileSystem
   */
  protected static Configuration getVolumeManagerConfiguration(AccumuloConfiguration conf,
      final Configuration hadoopConf, final String filesystemURI) {

    final var cacheKey = new Pair<>(hadoopConf, filesystemURI);
    return HDFS_CONFIGS_FOR_VOLUME.get(cacheKey, (key) -> {

      Map<String,String> volumeHdfsConfigOverrides =
          conf.getAllPropertiesWithPrefixStripped(Property.INSTANCE_VOLUME_CONFIG_PREFIX).entrySet()
              .stream().filter(e -> e.getKey().startsWith(filesystemURI + "."))
              .collect(Collectors.toUnmodifiableMap(
                  e -> e.getKey().substring(filesystemURI.length() + 1), Entry::getValue));

      // use the original if no overrides exist
      if (volumeHdfsConfigOverrides.isEmpty()) {
        return hadoopConf;
      }

      Configuration volumeConfig = new Configuration(hadoopConf);
      volumeHdfsConfigOverrides.forEach((k, v) -> {
        log.info("Overriding property {}={} for volume {}", k, v, filesystemURI);
        volumeConfig.set(k, v);
      });
      return volumeConfig;
    });
  }

  protected static Stream<Entry<String,String>>
      findVolumeOverridesMissingVolume(AccumuloConfiguration conf, Set<String> definedVolumes) {
    return conf.getAllPropertiesWithPrefixStripped(Property.INSTANCE_VOLUME_CONFIG_PREFIX)
        .entrySet().stream()
        // log only configs where none of the volumes (with a dot) prefix its key
        .filter(e -> definedVolumes.stream().noneMatch(vol -> e.getKey().startsWith(vol + ".")));
  }

  public static VolumeManager get(AccumuloConfiguration conf, final Configuration hadoopConf)
      throws IOException {
    final Map<String,Volume> volumes = new HashMap<>();

    Set<String> volumeStrings = VolumeConfiguration.getVolumeUris(conf);

    findVolumeOverridesMissingVolume(conf, volumeStrings).forEach(
        e -> log.warn("Found no matching volume for volume config override property {}", e));

    // The "default" Volume for Accumulo (in case no volumes are specified)
    for (String volumeUriOrDir : volumeStrings) {
      if (volumeUriOrDir.isBlank()) {
        throw new IllegalArgumentException("Empty volume specified in configuration");
      }

      if (volumeUriOrDir.startsWith("viewfs")) {
        throw new IllegalArgumentException("Cannot use viewfs as a volume");
      }

      // We require a URI here, fail if it doesn't look like one
      if (volumeUriOrDir.contains(":")) {
        Configuration volumeConfig =
            getVolumeManagerConfiguration(conf, hadoopConf, volumeUriOrDir);
        volumes.put(volumeUriOrDir, new VolumeImpl(new Path(volumeUriOrDir), volumeConfig));
      } else {
        throw new IllegalArgumentException("Expected fully qualified URI for "
            + Property.INSTANCE_VOLUMES.getKey() + " got " + volumeUriOrDir);
      }
    }

    return new VolumeManagerImpl(volumes, conf, hadoopConf);
  }

  @SuppressWarnings("deprecation")
  private static boolean inSafeMode(DistributedFileSystem dfs) throws IOException {
    // Returns true when safemode is on; this version of setSafeMode was deprecated in Hadoop 3.3.6,
    // because SafeModeAction enum was moved to a new package, and this deprecated method was
    // overloaded with a version of the method that accepts the new enum. However, we can't use that
    // replacement method if we want to continue working with versions less than 3.3.6, so we just
    // suppress the deprecation warning.
    return dfs.setSafeMode(SafeModeAction.SAFEMODE_GET);
  }

  @Override
  public boolean isReady() throws IOException {
    for (Volume volume : volumesByName.values()) {
      final FileSystem fs = volume.getFileSystem();
      if (fs instanceof DistributedFileSystem && inSafeMode((DistributedFileSystem) fs)) {
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
  public String choose(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
      Set<String> options) {
    final String choice;
    choice = chooser.choose(env, options);
    if (!options.contains(choice)) {
      String msg = "The configured volume chooser, '" + chooser.getClass()
          + "', or one of its delegates returned a volume not in the set of options provided";
      throw new IllegalStateException(msg);
    }
    return choice;
  }

  @Override
  public Set<String> choosable(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
      Set<String> options) {
    final Set<String> choices = chooser.choosable(env, options);
    for (String choice : choices) {
      if (!options.contains(choice)) {
        String msg = "The configured volume chooser, '" + chooser.getClass()
            + "', or one of its delegates returned a volume not in the set of options provided";
        throw new IllegalStateException(msg);
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
    if (fs instanceof DistributedFileSystem dfs) {
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
  public Collection<Volume> getVolumes() {
    return volumesByName.values();
  }
}
