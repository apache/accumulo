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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class FindCompactionTmpFiles {

  private static final Logger LOG = LoggerFactory.getLogger(FindCompactionTmpFiles.class);

  static class Opts extends ServerUtilOpts {

    @Parameter(names = "--tables", description = "comma separated list of table names")
    String tables;

    @Parameter(names = "--delete", description = "if true, will delete tmp files")
    boolean delete = false;
  }

  public static Set<Path> findTempFiles(ServerContext context, String tableId)
      throws InterruptedException {
    String tablePattern = tableId != null ? tableId : "*";
    final String pattern = "/tables/" + tablePattern + "/*/*";
    final Collection<Volume> vols = context.getVolumeManager().getVolumes();
    final ExecutorService svc = Executors.newFixedThreadPool(vols.size());
    final Set<Path> matches = new ConcurrentSkipListSet<>();
    final List<Future<Void>> futures = new ArrayList<>(vols.size());
    for (Volume vol : vols) {
      final Path volPattern = new Path(vol.getBasePath() + pattern);
      LOG.trace("Looking for tmp files that match pattern: {}", volPattern);
      futures.add(svc.submit(() -> {
        try {
          FileStatus[] files = vol.getFileSystem().globStatus(volPattern,
              (p) -> p.getName().contains("_tmp_" + ExternalCompactionId.PREFIX));
          Arrays.stream(files).forEach(fs -> matches.add(fs.getPath()));
        } catch (IOException e) {
          LOG.error("Error looking for tmp files in volume: {}", vol, e);
        }
        return null;
      }));
    }
    svc.shutdown();

    while (futures.size() > 0) {
      Iterator<Future<Void>> iter = futures.iterator();
      while (iter.hasNext()) {
        Future<Void> future = iter.next();
        if (future.isDone()) {
          iter.remove();
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error getting list of tmp files", e);
          }
        }
      }
      if (futures.size() > 0) {
        UtilWaitThread.sleep(3_000);
      }
    }
    svc.awaitTermination(10, TimeUnit.MINUTES);
    LOG.trace("Found compaction tmp files: {}", matches);

    // Remove paths of all active external compaction output files from the set of
    // tmp files found on the filesystem. This must be done *after* gathering the
    // matches on the filesystem.
    for (DataLevel level : DataLevel.values()) {
      context.getAmple().readTablets().forLevel(level).fetch(ColumnType.ECOMP).build()
          .forEach(tm -> {
            tm.getExternalCompactions().values()
                .forEach(ecm -> matches.remove(ecm.getCompactTmpName().getPath()));
          });
    }
    LOG.trace("Final set of compaction tmp files after removing active compactions: {}", matches);
    return matches;
  }

  public static class DeleteStats {
    public int success = 0;
    public int failure = 0;
    public int error = 0;
  }

  public static DeleteStats deleteTempFiles(ServerContext context, Set<Path> filesToDelete)
      throws InterruptedException {

    final ExecutorService delSvc = Executors.newFixedThreadPool(8);
    final List<Future<Boolean>> futures = new ArrayList<>(filesToDelete.size());
    final DeleteStats stats = new DeleteStats();

    filesToDelete.forEach(p -> {
      futures.add(delSvc.submit(() -> {
        if (context.getVolumeManager().exists(p)) {
          return context.getVolumeManager().delete(p);
        }
        return true;
      }));
    });
    delSvc.shutdown();

    int expectedResponses = filesToDelete.size();
    while (expectedResponses > 0) {
      Iterator<Future<Boolean>> iter = futures.iterator();
      while (iter.hasNext()) {
        Future<Boolean> future = iter.next();
        if (future.isDone()) {
          expectedResponses--;
          iter.remove();
          try {
            if (future.get()) {
              stats.success++;
            } else {
              stats.failure++;
            }
          } catch (ExecutionException e) {
            stats.error++;
            LOG.error("Error deleting a compaction tmp file", e);
          }
        }
      }
      LOG.debug("Waiting on {} background delete operations", expectedResponses);
      if (expectedResponses > 0) {
        UtilWaitThread.sleep(3_000);
      }
    }
    delSvc.awaitTermination(10, TimeUnit.MINUTES);
    return stats;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(FindCompactionTmpFiles.class.getName(), args);
    LOG.info("Looking for compaction tmp files over tables: {}, deleting: {}", opts.tables,
        opts.delete);

    Span span = TraceUtil.startSpan(FindCompactionTmpFiles.class, "main");
    try (Scope scope = span.makeCurrent()) {

      ServerContext context = opts.getServerContext();
      String[] tables = opts.tables.split(",");

      for (String table : tables) {

        table = table.trim();
        String tableId = context.tableOperations().tableIdMap().get(table);
        if (tableId == null || tableId.isEmpty()) {
          LOG.warn("TableId for table: {} does not exist, maybe the table was deleted?", table);
          continue;
        }

        final Set<Path> matches = findTempFiles(context, tableId);
        LOG.info("Found the following compaction tmp files for table {}:", table);
        matches.forEach(p -> LOG.info("{}", p));

        if (opts.delete) {
          LOG.info("Deleting compaction tmp files for table {}...", table);
          DeleteStats stats = deleteTempFiles(context, matches);
          LOG.info(
              "Deletion of compaction tmp files for table {} complete. Success:{}, Failure:{}, Error:{}",
              table, stats.success, stats.failure, stats.error);
        }

      }

    } finally {
      span.end();
    }
  }

}
