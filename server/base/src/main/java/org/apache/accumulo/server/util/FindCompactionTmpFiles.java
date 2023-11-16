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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class FindCompactionTmpFiles {

  private static final Logger LOG = LoggerFactory.getLogger(FindCompactionTmpFiles.class);

  static class Opts extends ServerUtilOpts {
    @Parameter(names = "--delete")
    boolean delete = false;
  }

  private static boolean allCompactorsDown(ClientContext context) {
    // This is a copy of ExternalCompactionUtil.getCompactorAddrs that returns
    // false if any compactor address is found. If there are no compactor addresses
    // in any of the groups, then it returns true.
    try {
      final Map<String,List<HostAndPort>> groupsAndAddresses = new HashMap<>();
      final String compactorGroupsPath = context.getZooKeeperRoot() + Constants.ZCOMPACTORS;
      ZooReader zooReader = context.getZooReader();
      List<String> groups = zooReader.getChildren(compactorGroupsPath);
      for (String group : groups) {
        groupsAndAddresses.putIfAbsent(group, new ArrayList<>());
        try {
          List<String> compactors = zooReader.getChildren(compactorGroupsPath + "/" + group);
          for (String compactor : compactors) {
            // compactor is the address, we are checking to see if there is a child node which
            // represents the compactor's lock as a check that it's alive.
            List<String> children =
                zooReader.getChildren(compactorGroupsPath + "/" + group + "/" + compactor);
            if (!children.isEmpty()) {
              LOG.trace("Found live compactor {} ", compactor);
              return false;
            }
          }
        } catch (NoNodeException e) {
          LOG.trace("Ignoring node that went missing", e);
        }
      }
      return true;
    } catch (KeeperException e) {
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  public static List<Path> findTempFiles(ServerContext context) throws InterruptedException {
    final String pattern = "/tables/*/*/*";
    final Collection<Volume> vols = context.getVolumeManager().getVolumes();
    final ExecutorService svc = Executors.newFixedThreadPool(vols.size());
    final List<Path> matches = new ArrayList<>(1024);
    final List<Future<Void>> futures = new ArrayList<>(vols.size());
    for (Volume vol : vols) {
      final Path volPattern = new Path(vol.getBasePath() + pattern);
      LOG.info("Looking for tmp files in volume: {} that match pattern: {}", vol, volPattern);
      futures.add(svc.submit(() -> {
        try {
          FileStatus[] files =
              vol.getFileSystem().globStatus(volPattern, (p) -> p.getName().contains("_tmp_ECID-"));
          System.out.println(Arrays.toString(files));
          Arrays.stream(files).forEach(fs -> matches.add(fs.getPath()));
        } catch (IOException e) {
          LOG.error("Error looking for tmp files in volume: {}", vol, e);
        }
        return null;
      }));
    }
    svc.shutdown();

    while (futures.size() > 0) {
      UtilWaitThread.sleep(10_000);
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
    }
    svc.awaitTermination(10, TimeUnit.MINUTES);
    return matches;
  }

  public static class DeleteStats {
    public int success = 0;
    public int failure = 0;
    public int error = 0;
  }

  public static DeleteStats deleteTempFiles(ServerContext context, List<Path> filesToDelete)
      throws InterruptedException {

    final ExecutorService delSvc = Executors.newFixedThreadPool(8);
    final List<Future<Boolean>> futures = new ArrayList<>(filesToDelete.size());
    final DeleteStats stats = new DeleteStats();

    filesToDelete.forEach(p -> {
      futures.add(delSvc.submit(() -> context.getVolumeManager().delete(p)));
    });
    delSvc.shutdown();

    int expectedResponses = filesToDelete.size();
    while (expectedResponses > 0) {
      UtilWaitThread.sleep(10_000);
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
      LOG.debug("Waiting on {} responses", expectedResponses);
    }
    delSvc.awaitTermination(10, TimeUnit.MINUTES);
    return stats;
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(FindCompactionTmpFiles.class.getName(), args);
    LOG.info("Deleting compaction tmp files: {}", opts.delete);
    Span span = TraceUtil.startSpan(FindCompactionTmpFiles.class, "main");
    try (Scope scope = span.makeCurrent()) {
      ServerContext context = opts.getServerContext();
      if (!allCompactorsDown(context)) {
        LOG.warn("Compactor addresses found in ZooKeeper. Unable to run this utility.");
      }

      final List<Path> matches = findTempFiles(context);
      LOG.info("Found the following compaction tmp files:");
      matches.forEach(p -> LOG.info("{}", p));

      if (opts.delete) {
        LOG.info("Deleting compaction tmp files...");
        DeleteStats stats = deleteTempFiles(context, matches);
        LOG.info("Deletion complete. Success:{}, Failure:{}, Error:{}", stats.success,
            stats.failure, stats.error);
      }

    } finally {
      span.end();
    }
  }

}
