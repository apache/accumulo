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
package org.apache.accumulo.server.util;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;

/**
 * This utility will remove scan server file references from the metadata table where the scan
 * server in the metadata entry is not currently running.
 */
public class ScanServerMetadataEntries {

  public static void clean(ServerContext context) {

    // the UUID present in the metadata table
    Set<UUID> uuidsToDelete = new HashSet<>();

    // collect all uuids that are currently in the metadata table
    context.getAmple().getScanServerFileReferences().forEach(ssrtf -> {
      uuidsToDelete.add(UUID.fromString(ssrtf.getServerLockUUID().toString()));
    });

    // gather the list of current live scan servers, its important that this is done after the above
    // step in order to avoid removing new scan servers that start while the method is running
    final Set<UUID> scanServerUuids = context.getScanServers().values().stream()
        .map(ssi -> ssi.getFirst()).collect(Collectors.toSet());

    // remove all live scan servers from the uuids seen in the metadata table... what is left is
    // uuids for scan servers that are dead
    uuidsToDelete.removeAll(scanServerUuids);

    if (!uuidsToDelete.isEmpty()) {
      final Set<ScanServerRefTabletFile> refsToDelete = new HashSet<>();

      context.getAmple().getScanServerFileReferences().forEach(ssrtf -> {

        var uuid = UUID.fromString(ssrtf.getServerLockUUID().toString());

        if (uuidsToDelete.contains(uuid)) {
          refsToDelete.add(ssrtf);
          if (refsToDelete.size() > 5000) {
            context.getAmple().deleteScanServerFileReferences(refsToDelete);
            refsToDelete.clear();
          }
        }
      });
      context.getAmple().deleteScanServerFileReferences(refsToDelete);
    }
  }

  public static void main(String[] args) {

    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(ScanServerMetadataEntries.class.getName(), args);

    final ServerContext context = opts.getServerContext();
    clean(context);
  }
}
