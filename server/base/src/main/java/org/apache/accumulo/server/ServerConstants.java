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
package org.apache.accumulo.server;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

public class ServerConstants {

  public static final String VERSION_DIR = "version";

  public static final String INSTANCE_ID_DIR = "instance_id";

  /**
   * version (10) reflects changes to how root tablet metadata is serialized in zookeeper starting
   * with 2.1
   */
  public static final int ROOT_TABLET_META_CHANGES = 10;

  /**
   * version (9) reflects changes to crypto that resulted in RFiles and WALs being serialized
   * differently in version 2.0.0. Also RFiles in 2.0.0 may have summary data.
   */
  public static final int CRYPTO_CHANGES = 9;

  /**
   * version (8) reflects changes to RFile index (ACCUMULO-1124) AND the change to WAL tracking in
   * ZK in version 1.8.0
   */
  public static final int SHORTEN_RFILE_KEYS = 8;

  /**
   * Historic data versions
   *
   * <ul>
   * <li>version (7) also reflects the addition of a replication table in 1.7.0
   * <li>version (6) reflects the addition of a separate root table (ACCUMULO-1481) in 1.6.0 -
   * <li>version (5) moves delete file markers for the metadata table into the root tablet
   * <li>version (4) moves logging to HDFS in 1.5.0
   * </ul>
   *
   *
   */
  public static final int DATA_VERSION = ROOT_TABLET_META_CHANGES;

  public static final Set<Integer> CAN_RUN =
      Set.of(SHORTEN_RFILE_KEYS, CRYPTO_CHANGES, DATA_VERSION);
  public static final Set<Integer> NEEDS_UPGRADE = Sets.difference(CAN_RUN, Set.of(DATA_VERSION));

  private static Set<String> baseUris = null;

  private static List<Pair<Path,Path>> replacementsList = null;

  public static Set<String> getBaseUris(ServerContext context) {
    return getBaseUris(context.getConfiguration(), context.getHadoopConf());
  }

  // these are functions to delay loading the Accumulo configuration unless we must
  public static synchronized Set<String> getBaseUris(AccumuloConfiguration conf,
      Configuration hadoopConf) {
    if (baseUris == null) {
      baseUris = Collections.unmodifiableSet(checkBaseUris(conf, hadoopConf,
          VolumeConfiguration.getVolumeUris(conf, hadoopConf), false));
    }

    return baseUris;
  }

  public static Set<String> checkBaseUris(AccumuloConfiguration conf, Configuration hadoopConf,
      Set<String> configuredBaseDirs, boolean ignore) {
    // all base dirs must have same instance id and data version, any dirs that have neither should
    // be ignored
    String firstDir = null;
    String firstIid = null;
    Integer firstVersion = null;
    // preserve order from configuration (to match user expectations a bit when volumes get sent to
    // user-implemented VolumeChoosers)
    LinkedHashSet<String> baseDirsList = new LinkedHashSet<>();
    for (String baseDir : configuredBaseDirs) {
      Path path = new Path(baseDir, INSTANCE_ID_DIR);
      String currentIid;
      int currentVersion;
      try {
        currentIid = VolumeManager.getInstanceIDFromHdfs(path, conf, hadoopConf);
        Path vpath = new Path(baseDir, VERSION_DIR);
        currentVersion =
            ServerUtil.getAccumuloPersistentVersion(vpath.getFileSystem(hadoopConf), vpath);
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

  public static final String TABLE_DIR = "tables";
  public static final String RECOVERY_DIR = "recovery";
  public static final String WAL_DIR = "wal";

  public static Set<String> getTablesDirs(ServerContext context) {
    return VolumeConfiguration.prefix(getBaseUris(context), TABLE_DIR);
  }

  public static Set<String> getRecoveryDirs(ServerContext context) {
    return VolumeConfiguration.prefix(getBaseUris(context), RECOVERY_DIR);
  }

  public static Path getInstanceIdLocation(Volume v) {
    // all base dirs should have the same instance id, so can choose any one
    return v.prefixChild(INSTANCE_ID_DIR);
  }

  public static Path getDataVersionLocation(Volume v) {
    // all base dirs should have the same version, so can choose any one
    return v.prefixChild(VERSION_DIR);
  }

  public static synchronized List<Pair<Path,Path>> getVolumeReplacements(AccumuloConfiguration conf,
      Configuration hadoopConf) {

    if (replacementsList == null) {
      String replacements = conf.get(Property.INSTANCE_VOLUMES_REPLACEMENTS);

      replacements = replacements.trim();

      if (replacements.isEmpty()) {
        return Collections.emptyList();
      }

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
      for (String baseDir : getBaseUris(conf, hadoopConf)) {
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
    }
    return replacementsList;
  }
}
