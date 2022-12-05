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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.util.DurationFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Read legacy properties (pre 2.1) from ZooKeeper and transform them into the single node format.
 * The encoded properties are stored in ZooKeeper and then the legacy property ZooKeeper nodes are
 * deleted.
 */
public class ConfigTransformer {

  private static final Logger log = LoggerFactory.getLogger(ConfigTransformer.class);

  private final ZooReaderWriter zrw;
  private final VersionedPropCodec codec;
  private final PropStoreWatcher propStoreWatcher;
  private final Retry retry;

  /**
   * Instantiate a transformer instance.
   *
   * @param zrw a ZooReaderWriter
   * @param codec the codec used to encode to the single-node format.
   * @param propStoreWatcher the watcher registered to receive future notifications of changes to
   *        the encoded property node.
   */
  public ConfigTransformer(final ZooReaderWriter zrw, VersionedPropCodec codec,
      final PropStoreWatcher propStoreWatcher) {
    this.zrw = zrw;
    this.codec = codec;
    this.propStoreWatcher = propStoreWatcher;

    // default - allow for a conservative max delay of about a minute
    retry =
        Retry.builder().maxRetries(15).retryAfter(250, MILLISECONDS).incrementBy(500, MILLISECONDS)
            .maxWait(5, SECONDS).backOffFactor(1.75).logInterval(3, MINUTES).createRetry();

  }

  public ConfigTransformer(final ZooReaderWriter zrw, VersionedPropCodec codec,
      final PropStoreWatcher propStoreWatcher, final Retry retry) {
    this.zrw = zrw;
    this.codec = codec;
    this.propStoreWatcher = propStoreWatcher;
    this.retry = retry;
  }

  /**
   * Transform the properties for the provided prop cache key.
   *
   * @return the encoded properties.
   */
  public VersionedProperties transform(final PropStoreKey<?> propStoreKey, final String legacyPath,
      final boolean deleteLegacyNode) {
    VersionedProperties exists = checkNeedsTransform(propStoreKey);
    if (exists != null) {
      return exists;
    }
    TransformToken token = TransformToken.createToken(legacyPath, zrw);
    return transform(propStoreKey, token, legacyPath, deleteLegacyNode);
  }

  // Allow external (mocked) TransformToken to be used
  @VisibleForTesting
  VersionedProperties transform(final PropStoreKey<?> propStoreKey, final TransformToken token,
      final String legacyPath, final boolean deleteLegacyNode) {
    log.trace("checking for legacy property upgrade transform for {}", propStoreKey);
    VersionedProperties results;
    Instant start = Instant.now();
    try {

      // check for node - just return if it exists.
      results = checkNeedsTransform(propStoreKey);
      if (results != null) {
        return results;
      }

      while (!token.haveTokenOwnership()) {
        try {
          retry.useRetry();
          retry.waitForNextAttempt(log, "transform property at " + propStoreKey.getPath());
          // look and return node if created while trying to token.
          log.trace("own the token - look for existing encoded node at: {}",
              propStoreKey.getPath());
          results = ZooPropStore.readFromZk(propStoreKey, propStoreWatcher, zrw);
          if (results != null) {
            log.trace(
                "Found existing node with properties after getting token at {}. skipping legacy prop conversion - version: {}, timestamp: {}",
                propStoreKey, results.getDataVersion(), results.getTimestamp());
            return results;
          }
          // still does not exist - try again.
          token.getTokenOwnership();
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Failed to hold transform token for " + propStoreKey, ex);
        } catch (IllegalStateException ex) {
          throw new IllegalStateException("Failed to hold transform token for " + propStoreKey, ex);
        }
      }

      Set<LegacyPropNode> upgradeNodes = readLegacyProps(legacyPath);
      if (upgradeNodes.size() == 0) {
        log.trace("No existing legacy props {}, skipping conversion, writing default prop node",
            propStoreKey);
        return writeNode(propStoreKey, Map.of());
      }

      upgradeNodes = convertDeprecatedProps(propStoreKey, upgradeNodes);

      results = writeConverted(propStoreKey, upgradeNodes);

      if (results == null) {
        throw new IllegalStateException("Could not create properties for " + propStoreKey);
      }

      // validate token still valid before deletion.
      if (!token.validateToken()) {
        throw new IllegalStateException(
            "legacy conversion failed. Lost transform token for " + propStoreKey);
      }

      Pair<Integer,Integer> deleteCounts = deleteLegacyProps(upgradeNodes);
      log.info("property transform for {} took {} ms, delete count: {}, error count: {}",
          propStoreKey, new DurationFormat(Duration.between(start, Instant.now()).toMillis(), ""),
          deleteCounts.getFirst(), deleteCounts.getSecond());

      return results;

    } catch (Exception ex) {
      log.info("Issue on upgrading legacy properties for: " + propStoreKey, ex);
    } finally {
      token.releaseToken();
      if (deleteLegacyNode) {
        log.trace("Delete legacy property base node: {}", legacyPath);
        try {
          zrw.delete(legacyPath);
        } catch (KeeperException.NotEmptyException ex) {
          log.info("Delete for legacy prop node {} - not empty", legacyPath);
        } catch (KeeperException | InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return null;
  }

  /**
   * If the config node exists, return the properties, otherwise return null. ZooKeeper exceptions
   * are ignored. Interrupt exceptions will be propagated as IllegalStateExceptions.
   *
   * @param propStoreKey the prop key for that identifies the configuration node.
   * @return the existing encoded properties if present, null if they do not.
   */
  private VersionedProperties checkNeedsTransform(PropStoreKey<?> propStoreKey) {
    try { // check for node - just return if it exists.
      VersionedProperties results = ZooPropStore.readFromZk(propStoreKey, propStoreWatcher, zrw);
      if (results != null) {
        log.trace(
            "Found existing node with properties at {}. skipping legacy prop conversion - version: {}, timestamp: {}",
            propStoreKey, results.getDataVersion(), results.getTimestamp());
        return results;
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during zookeeper read", ex);
    } catch (IOException | KeeperException ex) {
      log.trace("node for {} not found for upgrade", propStoreKey);
    }
    return null;
  }

  private Set<LegacyPropNode> convertDeprecatedProps(PropStoreKey<?> propStoreKey,
      Set<LegacyPropNode> upgradeNodes) {

    if (!(propStoreKey instanceof SystemPropKey)) {
      return upgradeNodes;
    }

    Set<LegacyPropNode> renamedNodes = new TreeSet<>();

    for (LegacyPropNode original : upgradeNodes) {
      var finalName = DeprecatedPropertyUtil.getReplacementName(original.getPropName(),
          (log, replacement) -> log
              .info("Automatically renaming deprecated property '{}' with its replacement '{}'"
                  + " in ZooKeeper configuration upgrade.", original, replacement));
      LegacyPropNode renamed = new LegacyPropNode(original.getPath(), finalName, original.getData(),
          original.getNodeVersion());
      renamedNodes.add(renamed);
    }
    return renamedNodes;
  }

  private @NonNull Set<LegacyPropNode> readLegacyProps(final String basePath) {

    Set<LegacyPropNode> legacyProps = new TreeSet<>();

    // strip leading slash
    var tokenName = TransformToken.TRANSFORM_TOKEN.substring(1);

    try {
      List<String> childNames = zrw.getChildren(basePath);
      for (String propName : childNames) {
        log.trace("processing ZooKeeper child node: {} at path: {}", propName, basePath);
        if (tokenName.equals(propName)) {
          continue;
        }

        log.trace("Adding: {} to list for legacy conversion", propName);

        var path = basePath + "/" + propName;
        Stat stat = new Stat();
        byte[] bytes = zrw.getData(path, stat);

        try {
          LegacyPropNode node;
          if (stat.getDataLength() > 0) {
            node = new LegacyPropNode(path, propName, new String(bytes, UTF_8), stat.getVersion());
          } else {
            node = new LegacyPropNode(path, propName, "", stat.getVersion());
          }
          legacyProps.add(node);
        } catch (IllegalStateException ex) {
          log.warn("Skipping invalid property at path " + path, ex);
        }
      }

    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to read legacy props due to ZooKeeper error", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Failed to read legacy props due to interrupt read from ZooKeeper", ex);
    }
    return legacyProps;
  }

  private Pair<Integer,Integer> deleteLegacyProps(Set<LegacyPropNode> nodes) {
    int deleteCount = 0;
    int errorCount = 0;
    for (LegacyPropNode n : nodes) {
      try {
        log.trace("Delete legacy prop at path: {}, data version: {}", n.getPath(),
            n.getNodeVersion());
        deleteCount++;
        zrw.deleteStrict(n.getPath(), n.getNodeVersion());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupt received during upgrade node clean-up", ex);
      } catch (KeeperException ex) {
        errorCount++;
        log.info("Failed to delete node during upgrade clean-up", ex);
      }
    }
    return new Pair<>(deleteCount, errorCount);
  }

  private @Nullable VersionedProperties writeConverted(final PropStoreKey<?> propStoreKey,
      final Set<LegacyPropNode> nodes) {
    final Map<String,String> props = new HashMap<>();
    nodes.forEach(node -> props.put(node.getPropName(), node.getData()));

    VersionedProperties vProps;
    try {
      vProps = writeNode(propStoreKey, props);
    } catch (InterruptedException | KeeperException ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException(
          "failed to create node for " + propStoreKey + " on conversion", ex);
    }

    if (!validateWrite(propStoreKey, vProps)) {
      log.trace("Failed property conversion validation for: {}", propStoreKey);
      // failed validation
      return null;
    }

    return vProps;
  }

  private VersionedProperties writeNode(final PropStoreKey<?> propStoreKey,
      final Map<String,String> props) throws InterruptedException, KeeperException {
    VersionedProperties vProps;
    try {
      String path = propStoreKey.getPath();
      log.trace("Writing converted properties to ZooKeeper path: {} for key: {}", path,
          propStoreKey);
      Stat currStat = zrw.getStatus(path);
      if (currStat == null || currStat.getDataLength() == 0) {
        // no node or node with no props stored
        vProps = new VersionedProperties(props);
        zrw.putPrivatePersistentData(path, codec.toBytes(vProps),
            ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
      return ZooPropStore.readFromZk(propStoreKey, propStoreWatcher, zrw);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "failed to create node for " + propStoreKey + " on conversion", ex);
    }
  }

  private boolean validateWrite(final PropStoreKey<?> propStoreKey,
      final VersionedProperties vProps) {
    try {
      Stat stat = zrw.getStatus(propStoreKey.getPath(), propStoreWatcher);
      if (stat == null) {
        throw new IllegalStateException(
            "failed to get stat to validate created node for " + propStoreKey);
      }
      log.debug("Property conversion validation - version received: {}, version expected: {}",
          stat.getVersion(), vProps.getDataVersion());
      return stat.getVersion() == vProps.getDataVersion();
    } catch (KeeperException ex) {
      throw new IllegalStateException("failed to validate created node for " + propStoreKey, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("failed to validate created node for " + propStoreKey, ex);
    }
  }

  /**
   * Immutable container for legacy ZooKeeper property node information.
   */
  private static class LegacyPropNode implements Comparable<LegacyPropNode> {
    private final String path;
    private final String propName;
    private final String data;
    private final int nodeVersion;

    /**
     * An immutable instance of legacy ZooKeeper property node information. It holds the property
     * and the node stat for later comparison to enable detection of ZooKeeper node changes. If the
     * legacy property name has been deprecated, the property is renamed and the conversion is noted
     * in the log.
     *
     * @param path the ZooKeeper path
     * @param propName the property name - if deprecated it will be stored as the updated name and
     *        the conversion logged.
     * @param data the property value
     * @param nodeVersion the ZooKeeper stat data version.
     */
    public LegacyPropNode(@NonNull final String path, final String propName, final String data,
        final int nodeVersion) {
      this.path = path;
      this.propName = propName;
      this.data = data;
      this.nodeVersion = nodeVersion;
    }

    public String getPath() {
      return path;
    }

    public String getPropName() {
      return propName;
    }

    public String getData() {
      return data;
    }

    public int getNodeVersion() {
      return nodeVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LegacyPropNode that = (LegacyPropNode) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path);
    }

    @Override
    public int compareTo(LegacyPropNode other) {
      return path.compareTo(other.path);
    }
  }

}
