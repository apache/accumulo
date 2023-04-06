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
package org.apache.accumulo.server;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class that holds important server Directories. These need to be separate from {@link ServerInfo}
 * for bootstrapping during initialization.
 */
public class ServerDirs {

  private Set<String> baseUris;
  private Set<String> tablesDirs;
  private Set<String> recoveryDirs;

  private final List<Pair<Path,Path>> replacementsList;
  private final AccumuloConfiguration conf;
  private final Configuration hadoopConf;

  public ServerDirs(AccumuloConfiguration conf, Configuration hadoopConf) {
    this.conf = Objects.requireNonNull(conf, "Configuration cannot be null");
    this.hadoopConf = Objects.requireNonNull(hadoopConf, "Hadoop configuration cannot be null");
    this.replacementsList = loadVolumeReplacements();
  }

  public Set<String> getBaseUris() {
    if (baseUris == null) {
      baseUris = Collections.unmodifiableSet(
          checkBaseUris(hadoopConf, VolumeConfiguration.getVolumeUris(conf), false));
    }
    return baseUris;
  }

  public Set<String> checkBaseUris(Configuration hadoopConf, Set<String> configuredBaseDirs,
      boolean ignore) {
    // all base dirs must have same instance id and data version, any dirs that have neither should
    // be ignored
    String firstDir = null;
    InstanceId firstIid = null;
    Integer firstVersion = null;
    // preserve order from configuration (to match user expectations a bit when volumes get sent to
    // user-implemented VolumeChoosers)
    LinkedHashSet<String> baseDirsList = new LinkedHashSet<>();
    for (String baseDir : configuredBaseDirs) {
      Path path = new Path(baseDir, Constants.INSTANCE_ID_DIR);
      InstanceId currentIid;
      int currentVersion;
      try {
        currentIid = VolumeManager.getInstanceIDFromHdfs(path, hadoopConf);
        Path vpath = new Path(baseDir, Constants.VERSION_DIR);
        currentVersion = getAccumuloPersistentVersion(vpath.getFileSystem(hadoopConf), vpath);
      } catch (Exception e) {
        if (ignore) {
          continue;
        } else {
          throw new IllegalArgumentException("Accumulo volume " + path + " not initialized", e);
        }
      }

      if (firstIid == null) {
        firstIid = currentIid;
        firstDir = baseDir;
        firstVersion = currentVersion;
      } else if (!currentIid.equals(firstIid)) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey()
            + " contains paths that have different instance ids " + baseDir + " has " + currentIid
            + " and " + firstDir + " has " + firstIid);
      } else if (currentVersion != firstVersion) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey()
            + " contains paths that have different versions " + baseDir + " has " + currentVersion
            + " and " + firstDir + " has " + firstVersion);
      }

      baseDirsList.add(baseDir);
    }

    if (baseDirsList.isEmpty()) {
      throw new RuntimeException("None of the configured paths are initialized.");
    }

    return baseDirsList;
  }

  private Set<String> prefix(Set<String> bases, String suffix) {
    String actualSuffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
    return bases.stream().map(base -> base + (base.endsWith("/") ? "" : "/") + actualSuffix)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  public Set<String> getTablesDirs() {
    if (tablesDirs == null) {
      tablesDirs = prefix(getBaseUris(), Constants.TABLE_DIR);
    }
    return tablesDirs;
  }

  public Set<String> getRecoveryDirs() {
    if (recoveryDirs == null) {
      recoveryDirs = prefix(getBaseUris(), Constants.RECOVERY_DIR);
    }
    return recoveryDirs;
  }

  private List<Pair<Path,Path>> loadVolumeReplacements() {

    List<Pair<Path,Path>> replacementsList;
    String replacements = conf.get(Property.INSTANCE_VOLUMES_REPLACEMENTS);

    if (replacements == null || replacements.trim().isEmpty()) {
      return Collections.emptyList();
    }
    replacements = replacements.trim();

    String[] pairs = replacements.split(",");
    List<Pair<Path,Path>> ret = new ArrayList<>();

    for (String pair : pairs) {

      String[] uris = pair.split("\\s+");
      if (uris.length != 2) {
        throw new IllegalArgumentException(
            Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains malformed pair " + pair);
      }

      Path p1, p2;
      try {
        // URI constructor handles hex escaping
        p1 = new Path(new URI(VolumeUtil.removeTrailingSlash(uris[0].trim())));
        if (p1.toUri().getScheme() == null) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey()
              + " contains " + uris[0] + " which is not fully qualified");
        }
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey()
            + " contains " + uris[0] + " which has a syntax error", e);
      }

      try {
        p2 = new Path(new URI(VolumeUtil.removeTrailingSlash(uris[1].trim())));
        if (p2.toUri().getScheme() == null) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey()
              + " contains " + uris[1] + " which is not fully qualified");
        }
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey()
            + " contains " + uris[1] + " which has a syntax error", e);
      }

      ret.add(new Pair<>(p1, p2));
    }

    HashSet<Path> baseDirs = new HashSet<>();
    for (String baseDir : getBaseUris()) {
      // normalize using path
      baseDirs.add(new Path(baseDir));
    }

    for (Pair<Path,Path> pair : ret) {
      if (!baseDirs.contains(pair.getSecond())) {
        throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey()
            + " contains " + pair.getSecond() + " which is not a configured volume");
      }
    }

    // only set if get here w/o exception
    replacementsList = ret;

    return replacementsList;
  }

  public List<Pair<Path,Path>> getVolumeReplacements() {
    return this.replacementsList;
  }

  public Path getDataVersionLocation(Volume v) {
    // all base dirs should have the same version, so can choose any one
    return v.prefixChild(Constants.VERSION_DIR);
  }

  public int getAccumuloPersistentVersion(Volume v) {
    Path path = getDataVersionLocation(v);
    return getAccumuloPersistentVersion(v.getFileSystem(), path);
  }

  public int getAccumuloPersistentVersion(FileSystem fs, Path path) {
    int dataVersion;
    try {
      FileStatus[] files = fs.listStatus(path);
      if (files == null || files.length == 0) {
        dataVersion = -1; // assume it is 0.5 or earlier
      } else {
        dataVersion = Integer.parseInt(files[0].getPath().getName());
      }
      return dataVersion;
    } catch (IOException e) {
      throw new RuntimeException("Unable to read accumulo version: an error occurred.", e);
    }
  }

  public Path getInstanceIdLocation(Volume v) {
    // all base dirs should have the same instance id, so can choose any one
    return v.prefixChild(Constants.INSTANCE_ID_DIR);
  }
}
