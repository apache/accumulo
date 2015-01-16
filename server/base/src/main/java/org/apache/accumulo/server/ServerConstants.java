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
package org.apache.accumulo.server;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.hadoop.fs.Path;

public class ServerConstants {

  public static final String VERSION_DIR = "version";

  public static final String INSTANCE_ID_DIR = "instance_id";

  /**
   * current version (3) reflects additional namespace operations (ACCUMULO-802) in version 1.6.0 <br />
   * (versions should never be negative)
   */
  public static final Integer WIRE_VERSION = 3;

  /**
   * current version (6) reflects the addition of a separate root table (ACCUMULO-1481) in version 1.6.0
   */
  public static final int DATA_VERSION = 6;
  public static final int PREV_DATA_VERSION = 5;
  public static final int TWO_DATA_VERSIONS_AGO = 4;

  private static String[] baseUris = null;

  private static List<Pair<Path,Path>> replacementsList = null;

  // these are functions to delay loading the Accumulo configuration unless we must
  public static synchronized String[] getBaseUris() {
    if (baseUris == null) {
      baseUris = checkBaseUris(VolumeConfiguration.getVolumeUris(ServerConfiguration.getSiteConfiguration()), false);
    }

    return baseUris;
  }

  public static String[] checkBaseUris(String[] configuredBaseDirs, boolean ignore) {
    // all base dirs must have same instance id and data version, any dirs that have neither should be ignored
    String firstDir = null;
    String firstIid = null;
    Integer firstVersion = null;
    ArrayList<String> baseDirsList = new ArrayList<String>();
    for (String baseDir : configuredBaseDirs) {
      Path path = new Path(baseDir, INSTANCE_ID_DIR);
      String currentIid;
      Integer currentVersion;
      try {
        currentIid = ZooUtil.getInstanceIDFromHdfs(path, ServerConfiguration.getSiteConfiguration());
        Path vpath = new Path(baseDir, VERSION_DIR);
        currentVersion = Accumulo.getAccumuloPersistentVersion(vpath.getFileSystem(CachedConfiguration.getInstance()), vpath);
      } catch (Exception e) {
        if (ignore)
          continue;
        else
          throw new IllegalArgumentException("Accumulo volume " + path + " not initialized", e);
      }

      if (firstIid == null) {
        firstIid = currentIid;
        firstDir = baseDir;
        firstVersion = currentVersion;
      } else if (!currentIid.equals(firstIid)) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey() + " contains paths that have different instance ids "
            + baseDir + " has " + currentIid + " and " + firstDir + " has " + firstIid);
      } else if (!currentVersion.equals(firstVersion)) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey() + " contains paths that have different versions " + baseDir
            + " has " + currentVersion + " and " + firstDir + " has " + firstVersion);
      }

      baseDirsList.add(baseDir);
    }

    if (baseDirsList.size() == 0) {
      throw new RuntimeException("None of the configured paths are initialized.");
    }

    return baseDirsList.toArray(new String[baseDirsList.size()]);
  }

  public static final String TABLE_DIR = "tables";
  public static final String RECOVERY_DIR = "recovery";
  public static final String WAL_DIR = "wal";
  public static final String WALOG_ARCHIVE_DIR = "walogArchive";
  public static final String FILE_ARCHIVE_DIR = "fileArchive";

  public static String[] getTablesDirs() {
    return VolumeConfiguration.prefix(getBaseUris(), TABLE_DIR);
  }

  public static String[] getRecoveryDirs() {
    return VolumeConfiguration.prefix(getBaseUris(), RECOVERY_DIR);
  }

  public static String[] getWalDirs() {
    return VolumeConfiguration.prefix(getBaseUris(), WAL_DIR);
  }

  public static String[] getWalogArchives() {
    return VolumeConfiguration.prefix(getBaseUris(), WALOG_ARCHIVE_DIR);
  }

  public static String[] getFileArchives() {
    return VolumeConfiguration.prefix(getBaseUris(), FILE_ARCHIVE_DIR);
  }

  public static Path getInstanceIdLocation(Volume v) {
    // all base dirs should have the same instance id, so can choose any one
    return v.prefixChild(INSTANCE_ID_DIR);
  }

  public static Path getDataVersionLocation(Volume v) {
    // all base dirs should have the same version, so can choose any one
    return v.prefixChild(VERSION_DIR);
  }

  public static synchronized List<Pair<Path,Path>> getVolumeReplacements() {

    if (replacementsList == null) {
      String replacements = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES_REPLACEMENTS);

      replacements = replacements.trim();

      if (replacements.isEmpty())
        return Collections.emptyList();

      String[] pairs = replacements.split(",");
      List<Pair<Path,Path>> ret = new ArrayList<Pair<Path,Path>>();

      for (String pair : pairs) {

        String uris[] = pair.split("\\s+");
        if (uris.length != 2)
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains malformed pair " + pair);

        Path p1, p2;
        try {
          // URI constructor handles hex escaping
          p1 = new Path(new URI(VolumeUtil.removeTrailingSlash(uris[0].trim())));
          if (p1.toUri().getScheme() == null)
            throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains " + uris[0] + " which is not fully qualified");
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains " + uris[0] + " which has a syntax error", e);
        }

        try {
          p2 = new Path(new URI(VolumeUtil.removeTrailingSlash(uris[1].trim())));
          if (p2.toUri().getScheme() == null)
            throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains " + uris[1] + " which is not fully qualified");
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains " + uris[1] + " which has a syntax error", e);
        }

        ret.add(new Pair<Path,Path>(p1, p2));
      }

      HashSet<Path> baseDirs = new HashSet<Path>();
      for (String baseDir : getBaseUris()) {
        // normalize using path
        baseDirs.add(new Path(baseDir));
      }

      for (Pair<Path,Path> pair : ret)
        if (!baseDirs.contains(pair.getSecond()))
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey() + " contains " + pair.getSecond()
              + " which is not a configured volume");

      // only set if get here w/o exception
      replacementsList = ret;
    }
    return replacementsList;
  }
}
