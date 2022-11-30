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
package org.apache.accumulo.server.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class governs the space in Zookeeper that advertises the status of Write-Ahead Logs in use
 * by tablet servers.
 *
 * <p>
 * The Accumulo Manager needs to know the state of the WALs to mark tablets during recovery. The GC
 * needs to know when a log is no longer needed so it can be removed.
 *
 * <p>
 * The state of the WALs is kept in Zookeeper under /accumulo/&lt;instanceid&gt;/wals. For each
 * server, there is a znode formatted like the TServerInstance.toString(): "host:port[sessionid]".
 * Under the server znode, is a node for each log, using the UUID for the log. In each of the WAL
 * znodes, is the current state of the log, and the full path to the log.
 *
 * <p>
 * The state [OPEN, CLOSED, UNREFERENCED] is what the tablet server believes to be the state of the
 * file.
 *
 * <p>
 * In the event of a recovery, the log is identified as belonging to a dead server. The manager will
 * update the tablets assigned to that server with log references. Once all tablets have been
 * reassigned and the log references are removed, the log will be eligible for deletion.
 *
 */
public class WalStateManager {

  public static class WalMarkerException extends Exception {
    private static final long serialVersionUID = 1L;

    public WalMarkerException(Exception ex) {
      super(ex);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(WalStateManager.class);

  public static final String ZWALS = "/wals";

  public enum WalState {
    /* log is open, and may be written to */
    OPEN,
    /* log is closed, and will not be written to again */
    CLOSED,
    /* unreferenced: no tablet needs the log for recovery */
    UNREFERENCED
  }

  private final ClientContext context;
  private final ZooReaderWriter zoo;

  private volatile boolean checkedExistance = false;

  public WalStateManager(ServerContext context) {
    this.context = context;
    this.zoo = context.getZooReaderWriter();
  }

  private String root() throws WalMarkerException {
    String root = context.getZooKeeperRoot() + ZWALS;

    try {
      if (!checkedExistance && !zoo.exists(root)) {
        zoo.putPersistentData(root, new byte[0], NodeExistsPolicy.SKIP);
      }

      checkedExistance = true;
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }

    return root;
  }

  // Tablet server exists
  public void initWalMarker(TServerInstance tsi) throws WalMarkerException {
    byte[] data = new byte[0];

    try {
      zoo.putPersistentData(root() + "/" + tsi, data, NodeExistsPolicy.FAIL);
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }
  }

  // Tablet server opens a new WAL
  public void addNewWalMarker(TServerInstance tsi, Path path) throws WalMarkerException {
    updateState(tsi, path, WalState.OPEN);
  }

  private void updateState(TServerInstance tsi, Path path, WalState state)
      throws WalMarkerException {
    byte[] data = (state + "," + path).getBytes(UTF_8);
    try {
      NodeExistsPolicy policy = NodeExistsPolicy.OVERWRITE;
      if (state == WalState.OPEN) {
        policy = NodeExistsPolicy.FAIL;
      }
      log.debug("Setting {} to {}", path.getName(), state);
      zoo.putPersistentData(root() + "/" + tsi + "/" + path.getName(), data, policy);
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }
  }

  // Tablet server has no references to the WAL
  public void walUnreferenced(TServerInstance tsi, Path path) throws WalMarkerException {
    updateState(tsi, path, WalState.UNREFERENCED);
  }

  private static Pair<WalState,Path> parse(byte[] data) {
    String[] parts = new String(data, UTF_8).split(",");
    return new Pair<>(WalState.valueOf(parts[0]), new Path(parts[1]));
  }

  // Manager needs to know the logs for the given instance
  public List<Path> getWalsInUse(TServerInstance tsi) throws WalMarkerException {
    List<Path> result = new ArrayList<>();
    try {
      String zpath = root() + "/" + tsi;
      zoo.sync(zpath);
      for (String child : zoo.getChildren(zpath)) {
        byte[] zdata = null;
        try {
          // This function is called by the Manager. Its possible that Accumulo GC deletes an
          // unreferenced WAL in ZK after the call to getChildren above. Catch this exception inside
          // the loop so that not all children are ignored.
          zdata = zoo.getData(zpath + "/" + child);
        } catch (KeeperException.NoNodeException e) {
          log.debug("WAL state removed {} {} during getWalsInUse.  Likely a race condition between "
              + "manager and GC.", tsi, child);
        }

        if (zdata != null) {
          Pair<WalState,Path> parts = parse(zdata);
          if (parts.getFirst() != WalState.UNREFERENCED) {
            result.add(parts.getSecond());
          }
        }
      }
    } catch (KeeperException.NoNodeException e) {
      log.debug("{} has no wal entry in zookeeper, assuming no logs", tsi);
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }
    return result;
  }

  // garbage collector wants the list of logs markers for all servers
  public Map<TServerInstance,List<UUID>> getAllMarkers() throws WalMarkerException {
    Map<TServerInstance,List<UUID>> result = new HashMap<>();
    try {
      String path = root();
      for (String child : zoo.getChildren(path)) {
        TServerInstance inst = new TServerInstance(child);
        List<UUID> logs = result.computeIfAbsent(inst, k -> new ArrayList<>());

        // This function is called by the Accumulo GC which deletes WAL markers. Therefore we do not
        // expect the following call to fail because the WAL info in ZK was deleted.
        for (String idString : zoo.getChildren(path + "/" + child)) {
          logs.add(UUID.fromString(idString));
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }
    return result;
  }

  // garbage collector wants to know the state (open/closed) of a log, and the filename to delete
  public Pair<WalState,Path> state(TServerInstance instance, UUID uuid) throws WalMarkerException {
    try {
      String path = root() + "/" + instance + "/" + uuid;
      return parse(zoo.getData(path));
    } catch (KeeperException | InterruptedException e) {
      throw new WalMarkerException(e);
    }
  }

  // utility combination of getAllMarkers and state
  public Map<Path,WalState> getAllState() throws WalMarkerException {
    Map<Path,WalState> result = new HashMap<>();
    for (Entry<TServerInstance,List<UUID>> entry : getAllMarkers().entrySet()) {
      for (UUID id : entry.getValue()) {
        // This function is called by the Accumulo GC which deletes WAL markers. Therefore we do not
        // expect the following call to fail because the WAL info in ZK was deleted.
        Pair<WalState,Path> state = state(entry.getKey(), id);
        result.put(state.getSecond(), state.getFirst());
      }
    }
    return result;
  }

  // garbage collector knows it's safe to remove the marker for a closed log
  public void removeWalMarker(TServerInstance instance, UUID uuid) throws WalMarkerException {
    try {
      log.debug("Removing {}", uuid);
      String path = root() + "/" + instance + "/" + uuid;
      zoo.delete(path);
    } catch (InterruptedException | KeeperException e) {
      throw new WalMarkerException(e);
    }
  }

  // garbage collector knows the instance is dead, and has no markers
  public void forget(TServerInstance instance) throws WalMarkerException {
    String path = root() + "/" + instance;
    try {
      zoo.recursiveDelete(path, NodeMissingPolicy.FAIL);
    } catch (InterruptedException | KeeperException e) {
      throw new WalMarkerException(e);
    }
  }

  // tablet server can mark the log as closed (but still needed)
  // manager can mark a log as unreferenced after it has made log recovery markers on the tablets
  // that need to be recovered
  public void closeWal(TServerInstance instance, Path path) throws WalMarkerException {
    updateState(instance, path, WalState.CLOSED);
  }
}
