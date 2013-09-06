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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class VolumeManagerImpl implements VolumeManager {

  private static final Logger log = Logger.getLogger(VolumeManagerImpl.class);

  Map<String,? extends FileSystem> volumes;
  String defaultVolume;
  AccumuloConfiguration conf;
  VolumeChooser chooser;

  protected VolumeManagerImpl(Map<String,? extends FileSystem> volumes, String defaultVolume, AccumuloConfiguration conf) {
    this.volumes = volumes;
    this.defaultVolume = defaultVolume;
    this.conf = conf;
    ensureSyncIsEnabled();
    try {
      chooser = (VolumeChooser) this.getClass().getClassLoader().loadClass(conf.get(Property.GENERAL_VOLUME_CHOOSER)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static org.apache.accumulo.server.fs.VolumeManager getLocal() throws IOException {
    return new VolumeManagerImpl(Collections.singletonMap("", FileSystem.getLocal(CachedConfiguration.getInstance())), "",
        DefaultConfiguration.getDefaultConfiguration());
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    for (FileSystem fs : volumes.values()) {
      try {
        fs.close();
      } catch (IOException e) {
        ex = e;
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public boolean closePossiblyOpenFile(Path path) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        return dfs.recoverLease(path);
      } catch (FileNotFoundException ex) {
        throw ex;
      }
    } else if (fs instanceof LocalFileSystem) {
      // ignore
    } else {
      throw new IllegalStateException("Don't know how to recover a lease for " + fs.getClass().getName());
    }
    fs.append(path).close();
    log.info("Recovered lease on " + path.toString() + " using append");
    return true;
  }

  @Override
  public FSDataOutputStream create(Path path) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    return fs.create(path);
  }

  @Override
  public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    return fs.create(path, overwrite);
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
  public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    if (bufferSize == 0) {
      fs.getConf().getInt("io.file.buffer.size", 4096);
    }
    return fs.create(path, overwrite, bufferSize, replication, correctBlockSize(fs.getConf(), blockSize));
  }

  @Override
  public boolean createNewFile(Path path) throws IOException {
    FileSystem fs = getFileSystemByPath(path);
    return fs.createNewFile(path);
  }

  @Override
  public FSDataOutputStream createSyncable(Path logPath, int bufferSize, short replication, long blockSize) throws IOException {
    FileSystem fs = getFileSystemByPath(logPath);
    blockSize = correctBlockSize(fs.getConf(), blockSize);
    bufferSize = correctBufferSize(fs.getConf(), bufferSize);
    try {
      // This...
      // EnumSet<CreateFlag> set = EnumSet.of(CreateFlag.SYNC_BLOCK, CreateFlag.CREATE);
      // return fs.create(logPath, FsPermission.getDefault(), set, buffersize, replication, blockSize, null);
      // Becomes this:
      Class<?> createFlags = Class.forName("org.apache.hadoop.fs.CreateFlag");
      List<Enum<?>> flags = new ArrayList<Enum<?>>();
      if (createFlags.isEnum()) {
        for (Object constant : createFlags.getEnumConstants()) {
          if (constant.toString().equals("SYNC_BLOCK")) {
            flags.add((Enum<?>) constant);
            log.debug("Found synch enum " + constant);
          }
          if (constant.toString().equals("CREATE")) {
            flags.add((Enum<?>) constant);
            log.debug("Found CREATE enum " + constant);
          }
        }
      }
      Object set = EnumSet.class.getMethod("of", java.lang.Enum.class, java.lang.Enum.class).invoke(null, flags.get(0), flags.get(1));
      log.debug("CreateFlag set: " + set);
      Method create = fs.getClass().getMethod("create", Path.class, FsPermission.class, EnumSet.class, Integer.TYPE, Short.TYPE, Long.TYPE, Progressable.class);
      log.debug("creating " + logPath + " with SYNCH_BLOCK flag");
      return (FSDataOutputStream) create.invoke(fs, logPath, FsPermission.getDefault(), set, bufferSize, replication, blockSize, null);
    } catch (ClassNotFoundException ex) {
      // Expected in hadoop 1.0
      return fs.create(logPath, true, bufferSize, replication, blockSize);
    } catch (Exception ex) {
      log.debug(ex, ex);
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

  private void ensureSyncIsEnabled() {
    for (FileSystem fs : getFileSystems().values()) {
      if (fs instanceof DistributedFileSystem) {
        if (!fs.getConf().getBoolean("dfs.durable.sync", false) && !fs.getConf().getBoolean("dfs.support.append", false)) {
          String msg = "Must set dfs.durable.sync OR dfs.support.append to true.  Which one needs to be set depends on your version of HDFS.  See ACCUMULO-623. \n"
              + "HADOOP RELEASE          VERSION           SYNC NAME             DEFAULT\n"
              + "Apache Hadoop           0.20.205          dfs.support.append    false\n"
              + "Apache Hadoop            0.23.x           dfs.support.append    true\n"
              + "Apache Hadoop             1.0.x           dfs.support.append    false\n"
              + "Apache Hadoop             1.1.x           dfs.durable.sync      true\n"
              + "Apache Hadoop          2.0.0-2.0.2        dfs.support.append    true\n"
              + "Cloudera CDH             3u0-3u3             ????               true\n"
              + "Cloudera CDH               3u4            dfs.support.append    true\n"
              + "Hortonworks HDP           `1.0            dfs.support.append    false\n"
              + "Hortonworks HDP           `1.1            dfs.support.append    false";
          log.fatal(msg);
          System.exit(-1);
        }
        try {
          // if this class exists
          Class.forName("org.apache.hadoop.fs.CreateFlag");
          // we're running hadoop 2.0, 1.1
          if (!fs.getConf().getBoolean("dfs.datanode.synconclose", false)) {
            log.warn("dfs.datanode.synconclose set to false: data loss is possible on system reset or power loss");
          }
        } catch (ClassNotFoundException ex) {
          // hadoop 1.0
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
    if (path.toString().contains(":")) {
      try {
        return path.getFileSystem(CachedConfiguration.getInstance());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return volumes.get(defaultVolume);
  }

  @Override
  public Map<String,? extends FileSystem> getFileSystems() {
    return volumes;
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
  public FSDataInputStream open(Path path) throws IOException {
    return getFileSystemByPath(path).open(path);
  }

  @Override
  public boolean rename(Path path, Path newPath) throws IOException {
    FileSystem source = getFileSystemByPath(path);
    FileSystem dest = getFileSystemByPath(newPath);
    if (source != dest) {
      throw new NotImplementedException("Cannot rename files across volumes: " + path + " -> " + newPath);
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
    @SuppressWarnings("deprecation")
    short rep = getFileSystemByPath(path).getDefaultReplication();
    return rep;
  }

  @Override
  public boolean isFile(Path path) throws IOException {
    return getFileSystemByPath(path).isFile(path);
  }

  public static VolumeManager get() throws IOException {
    AccumuloConfiguration conf = ServerConfiguration.getSystemConfiguration(HdfsZooInstance.getInstance());
    return get(conf);
  }

  static private final String DEFAULT = "";

  public static VolumeManager get(AccumuloConfiguration conf) throws IOException {
    Map<String,FileSystem> fileSystems = new HashMap<String,FileSystem>();
    Configuration hadoopConf = CachedConfiguration.getInstance();
    fileSystems.put(DEFAULT, FileSystem.get(hadoopConf));
    String ns = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES);
    if (ns != null) {
      for (String space : ns.split(",")) {
        if (space.contains(":")) {
          fileSystems.put(space, new Path(space).getFileSystem(hadoopConf));
        } else {
          fileSystems.put(space, FileSystem.get(hadoopConf));
        }
      }
    }
    return new VolumeManagerImpl(fileSystems, "", conf);
  }

  @Override
  public boolean isReady() throws IOException {
    for (FileSystem fs : getFileSystems().values()) {
      if (!(fs instanceof DistributedFileSystem))
        continue;
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      // So this: if (!dfs.setSafeMode(SafeModeAction.SAFEMODE_GET))
      // Becomes this:
      Class<?> safeModeAction;
      try {
        // hadoop 2.0
        safeModeAction = Class.forName("org.apache.hadoop.hdfs.protocol.HdfsConstants$SafeModeAction");
      } catch (ClassNotFoundException ex) {
        // hadoop 1.0
        try {
          safeModeAction = Class.forName("org.apache.hadoop.hdfs.protocol.FSConstants$SafeModeAction");
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Cannot figure out the right class for Constants");
        }
      }
      Object get = null;
      for (Object obj : safeModeAction.getEnumConstants()) {
        if (obj.toString().equals("SAFEMODE_GET"))
          get = obj;
      }
      if (get == null) {
        throw new RuntimeException("cannot find SAFEMODE_GET");
      }
      try {
        Method setSafeMode = dfs.getClass().getMethod("setSafeMode", safeModeAction);
        boolean inSafeMode = (Boolean) setSafeMode.invoke(dfs, get);
        if (inSafeMode) {
          return false;
        }
      } catch (Exception ex) {
        throw new RuntimeException("cannot find method setSafeMode");
      }
    }
    return true;
  }

  @Override
  public FileSystem getDefaultVolume() {
    return volumes.get(defaultVolume);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return getFileSystemByPath(pathPattern).globStatus(pathPattern);
  }

  @Override
  public Path getFullPath(Key key) {

    String relPath = key.getColumnQualifierData().toString();
    if (relPath.contains(":"))
      return new Path(relPath);

    byte[] tableId = KeyExtent.tableOfMetadataRow(key.getRow());

    if (relPath.startsWith("../"))
      relPath = relPath.substring(2);
    else
      relPath = "/" + new String(tableId) + relPath;
    Path fullPath = new Path(ServerConstants.getTablesDirs()[0] + relPath);
    FileSystem fs = getFileSystemByPath(fullPath);
    return fs.makeQualified(fullPath);
  }

  @Override
  public Path matchingFileSystem(Path source, String[] options) {
    for (String fs : getFileSystems().keySet()) {
      for (String option : options) {
        if (option.startsWith(fs))
          return new Path(option);
      }
    }
    return null;
  }

  @Override
  public String newPathOnSameVolume(String sourceDir, String suffix) {
    for (String fs : getFileSystems().keySet()) {
      if (sourceDir.startsWith(fs)) {
        return fs + "/" + suffix;
      }
    }
    return null;
  }

  @Override
  public Path getFullPath(String[] paths, String fileName) throws IOException {
    if (fileName.contains(":"))
      return new Path(fileName);
    // TODO: ACCUMULO-118
    // How do we want it to work? Find it somewhere? or find it in the default file system?
    // old-style name, on one of many possible "root" paths:
    if (fileName.startsWith("../"))
      fileName = fileName.substring(2);
    for (String path : paths) {
      String fullPath;
      if (path.endsWith("/") || fileName.startsWith("/"))
        fullPath = path + fileName;
      else
        fullPath = path + "/" + fileName;
      Path exists = new Path(fullPath);
      FileSystem ns = getFileSystemByPath(exists);
      if (ns.exists(exists)) {
        Path result = ns.makeQualified(exists);
        log.debug("Found " + exists + " on " + path + " as " + result);
        return ns.makeQualified(exists);
      }
    }
    throw new IOException("Could not find file " + fileName + " in " + Arrays.asList(paths));
  }

  @Override
  public ContentSummary getContentSummary(Path dir) throws IOException {
    return getFileSystemByPath(dir).getContentSummary(dir);
  }

  @Override
  public String choose(String[] options) {
    return chooser.choose(options);
  }

}
