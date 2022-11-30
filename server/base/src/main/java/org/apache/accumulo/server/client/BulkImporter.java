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
package org.apache.accumulo.server.client;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.StopWatch;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkImporter {

  private static final Logger log = LoggerFactory.getLogger(BulkImporter.class);

  public static List<String> bulkLoad(ServerContext context, long tid, String tableId,
      List<String> files, boolean setTime) throws IOException {
    AssignmentStats stats = new BulkImporter(context, tid, tableId, setTime).importFiles(files);
    List<String> result = new ArrayList<>();
    for (Path p : stats.completeFailures.keySet()) {
      result.add(p.toString());
    }
    return result;
  }

  private StopWatch<Timers> timer;

  private enum Timers {
    EXAMINE_MAP_FILES, QUERY_METADATA, IMPORT_MAP_FILES, SLEEP, TOTAL
  }

  private final ServerContext context;
  private TableId tableId;
  private long tid;
  private boolean setTime;
  private TableConfiguration tableConf;

  public BulkImporter(ServerContext context, long tid, String tableId, boolean setTime) {
    this.context = context;
    this.tid = tid;
    this.tableId = TableId.of(tableId);
    this.setTime = setTime;
    this.tableConf = context.getTableConfiguration(this.tableId);
  }

  public AssignmentStats importFiles(List<String> files) {

    int numThreads = context.getConfiguration().getCount(Property.TSERV_BULK_PROCESS_THREADS);
    int numAssignThreads =
        context.getConfiguration().getCount(Property.TSERV_BULK_ASSIGNMENT_THREADS);

    timer = new StopWatch<>(Timers.class);
    timer.start(Timers.TOTAL);

    final VolumeManager fs = context.getVolumeManager();

    Set<Path> paths = new HashSet<>();
    for (String file : files) {
      paths.add(new Path(file));
    }
    AssignmentStats assignmentStats = new AssignmentStats(paths.size());

    final Map<Path,List<KeyExtent>> completeFailures =
        Collections.synchronizedSortedMap(new TreeMap<>());

    ClientService.Client client = null;
    final TabletLocator locator = TabletLocator.getLocator(context, tableId);

    try {
      final Map<Path,List<TabletLocation>> assignments =
          Collections.synchronizedSortedMap(new TreeMap<>());

      timer.start(Timers.EXAMINE_MAP_FILES);
      ExecutorService threadPool = ThreadPools.getServerThreadPools()
          .createFixedThreadPool(numThreads, "findOverlapping", false);

      for (Path path : paths) {
        final Path mapFile = path;
        Runnable getAssignments = () -> {
          List<TabletLocation> tabletsToAssignMapFileTo = Collections.emptyList();
          try {
            tabletsToAssignMapFileTo =
                findOverlappingTablets(context, fs, locator, mapFile, tableConf.getCryptoService());
          } catch (Exception ex) {
            log.warn("Unable to find tablets that overlap file " + mapFile, ex);
          }
          log.debug("Map file {} found to overlap {} tablets", mapFile,
              tabletsToAssignMapFileTo.size());
          if (tabletsToAssignMapFileTo.isEmpty()) {
            List<KeyExtent> empty = Collections.emptyList();
            completeFailures.put(mapFile, empty);
          } else {
            assignments.put(mapFile, tabletsToAssignMapFileTo);
          }

        };
        threadPool.execute(getAssignments);
      }
      threadPool.shutdown();
      while (!threadPool.isTerminated()) {
        try {
          threadPool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      timer.stop(Timers.EXAMINE_MAP_FILES);

      assignmentStats.attemptingAssignments(assignments);
      Map<Path,List<KeyExtent>> assignmentFailures =
          assignMapFiles(fs, assignments, paths, numAssignThreads, numThreads);
      assignmentStats.assignmentsFailed(assignmentFailures);

      Map<Path,Integer> failureCount = new TreeMap<>();

      for (Entry<Path,List<KeyExtent>> entry : assignmentFailures.entrySet()) {
        failureCount.put(entry.getKey(), 1);
      }

      long sleepTime = 2_000;
      while (!assignmentFailures.isEmpty()) {
        sleepTime = Math.min(sleepTime * 2, MINUTES.toMillis(1));
        locator.invalidateCache();
        // assumption about assignment failures is that it caused by a split
        // happening or a missing location
        //
        // for splits we need to find children key extents that cover the
        // same key range and are contiguous (no holes, no overlap)

        timer.start(Timers.SLEEP);
        sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
        timer.stop(Timers.SLEEP);

        log.debug("Trying to assign {} map files that previously failed on some key extents",
            assignmentFailures.size());
        assignments.clear();

        // for failed key extents, try to find children key extents to
        // assign to
        for (Entry<Path,List<KeyExtent>> entry : assignmentFailures.entrySet()) {
          Iterator<KeyExtent> keListIter = entry.getValue().iterator();

          List<TabletLocation> tabletsToAssignMapFileTo = new ArrayList<>();

          while (keListIter.hasNext()) {
            KeyExtent ke = keListIter.next();

            timer.start(Timers.QUERY_METADATA);
            try {
              tabletsToAssignMapFileTo.addAll(findOverlappingTablets(context, fs, locator,
                  entry.getKey(), ke, tableConf.getCryptoService()));
              keListIter.remove();
            } catch (Exception ex) {
              log.warn("Exception finding overlapping tablets, will retry tablet " + ke, ex);
            }
            timer.stop(Timers.QUERY_METADATA);
          }

          if (!tabletsToAssignMapFileTo.isEmpty()) {
            assignments.put(entry.getKey(), tabletsToAssignMapFileTo);
          }
        }

        assignmentStats.attemptingAssignments(assignments);
        Map<Path,List<KeyExtent>> assignmentFailures2 =
            assignMapFiles(fs, assignments, paths, numAssignThreads, numThreads);
        assignmentStats.assignmentsFailed(assignmentFailures2);

        // merge assignmentFailures2 into assignmentFailures
        for (Entry<Path,List<KeyExtent>> entry : assignmentFailures2.entrySet()) {
          assignmentFailures.get(entry.getKey()).addAll(entry.getValue());

          Integer fc = failureCount.get(entry.getKey());
          if (fc == null) {
            fc = 0;
          }

          failureCount.put(entry.getKey(), fc + 1);
        }

        // remove map files that have no more key extents to assign
        assignmentFailures.values().removeIf(List::isEmpty);

        Set<Entry<Path,Integer>> failureIter = failureCount.entrySet();
        for (Entry<Path,Integer> entry : failureIter) {
          int retries = context.getConfiguration().getCount(Property.TSERV_BULK_RETRY);
          if (entry.getValue() > retries && assignmentFailures.get(entry.getKey()) != null) {
            log.error("Map file {} failed more than {} times, giving up.", entry.getKey(), retries);
            completeFailures.put(entry.getKey(), assignmentFailures.get(entry.getKey()));
            assignmentFailures.remove(entry.getKey());
          }
        }
      }
      assignmentStats.assignmentsAbandoned(completeFailures);
      Set<Path> failedFailures = processFailures(completeFailures);
      assignmentStats.unrecoveredMapFiles(failedFailures);

      timer.stop(Timers.TOTAL);
      printReport(paths);
      return assignmentStats;
    } finally {
      if (client != null) {
        ThriftUtil.close(client, context);
      }
    }
  }

  private void printReport(Set<Path> paths) {
    long totalTime = 0;
    for (Timers t : Timers.values()) {
      if (t == Timers.TOTAL) {
        continue;
      }

      totalTime += timer.get(t);
    }
    List<String> files = new ArrayList<>();
    for (Path path : paths) {
      files.add(path.getName());
    }
    Collections.sort(files);

    log.debug("BULK IMPORT TIMING STATISTICS");
    log.debug("Files: {}", files);
    log.debug(String.format("Examine map files    : %,10.2f secs %6.2f%s",
        timer.getSecs(Timers.EXAMINE_MAP_FILES),
        100.0 * timer.get(Timers.EXAMINE_MAP_FILES) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Query %-14s : %,10.2f secs %6.2f%s", MetadataTable.NAME,
        timer.getSecs(Timers.QUERY_METADATA),
        100.0 * timer.get(Timers.QUERY_METADATA) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Import Map Files     : %,10.2f secs %6.2f%s",
        timer.getSecs(Timers.IMPORT_MAP_FILES),
        100.0 * timer.get(Timers.IMPORT_MAP_FILES) / timer.get(Timers.TOTAL), "%"));
    log.debug(
        String.format("Sleep                : %,10.2f secs %6.2f%s", timer.getSecs(Timers.SLEEP),
            100.0 * timer.get(Timers.SLEEP) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Misc                 : %,10.2f secs %6.2f%s",
        (timer.get(Timers.TOTAL) - totalTime) / 1000.0,
        100.0 * (timer.get(Timers.TOTAL) - totalTime) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Total                : %,10.2f secs", timer.getSecs(Timers.TOTAL)));
  }

  private Set<Path> processFailures(Map<Path,List<KeyExtent>> completeFailures) {
    // we should check if map file was not assigned to any tablets, then we
    // should just move it; not currently being done?

    Set<Entry<Path,List<KeyExtent>>> es = completeFailures.entrySet();

    if (completeFailures.isEmpty()) {
      return Collections.emptySet();
    }

    log.debug("The following map files failed ");

    for (Entry<Path,List<KeyExtent>> entry : es) {
      List<KeyExtent> extents = entry.getValue();

      for (KeyExtent keyExtent : extents) {
        log.debug("\t{} -> {}", entry.getKey(), keyExtent);
      }
    }

    return Collections.emptySet();
  }

  private static class AssignmentInfo {
    public AssignmentInfo(KeyExtent keyExtent, Long estSize) {
      this.ke = keyExtent;
      this.estSize = estSize;
    }

    KeyExtent ke;
    long estSize;
  }

  private static List<KeyExtent> extentsOf(List<TabletLocation> locations) {
    List<KeyExtent> result = new ArrayList<>(locations.size());
    for (TabletLocation tl : locations) {
      result.add(tl.tablet_extent);
    }
    return result;
  }

  private Map<Path,List<AssignmentInfo>> estimateSizes(final VolumeManager vm,
      Map<Path,List<TabletLocation>> assignments, Collection<Path> paths, int numThreads) {

    long t1 = System.currentTimeMillis();
    final Map<Path,Long> mapFileSizes = new TreeMap<>();

    try {
      for (Path path : paths) {
        FileSystem fs = vm.getFileSystemByPath(path);
        mapFileSizes.put(path, fs.getContentSummary(path).getLength());
      }
    } catch (IOException e) {
      log.error("Failed to get map files in for {}: {}", paths, e.getMessage(), e);
      throw new RuntimeException(e);
    }

    final Map<Path,List<AssignmentInfo>> ais = Collections.synchronizedMap(new TreeMap<>());

    ExecutorService threadPool = ThreadPools.getServerThreadPools()
        .createFixedThreadPool(numThreads, "estimateSizes", false);

    for (final Entry<Path,List<TabletLocation>> entry : assignments.entrySet()) {
      if (entry.getValue().size() == 1) {
        TabletLocation tabletLocation = entry.getValue().get(0);

        // if the tablet completely contains the map file, there is no
        // need to estimate its
        // size
        ais.put(entry.getKey(), Collections.singletonList(
            new AssignmentInfo(tabletLocation.tablet_extent, mapFileSizes.get(entry.getKey()))));
        continue;
      }

      Runnable estimationTask = () -> {
        Map<KeyExtent,Long> estimatedSizes = null;

        try {
          Path mapFile = entry.getKey();
          FileSystem ns = context.getVolumeManager().getFileSystemByPath(mapFile);

          estimatedSizes = BulkImport.estimateSizes(context.getConfiguration(), mapFile,
              mapFileSizes.get(entry.getKey()), extentsOf(entry.getValue()), ns, null,
              tableConf.getCryptoService());
        } catch (IOException e) {
          log.warn("Failed to estimate map file sizes {}", e.getMessage());
        }

        if (estimatedSizes == null) {
          // estimation failed, do a simple estimation
          estimatedSizes = new TreeMap<>();
          long estSize =
              (long) (mapFileSizes.get(entry.getKey()) / (double) entry.getValue().size());
          for (TabletLocation tl : entry.getValue()) {
            estimatedSizes.put(tl.tablet_extent, estSize);
          }
        }

        List<AssignmentInfo> assignmentInfoList = new ArrayList<>(estimatedSizes.size());

        for (Entry<KeyExtent,Long> entry2 : estimatedSizes.entrySet()) {
          assignmentInfoList.add(new AssignmentInfo(entry2.getKey(), entry2.getValue()));
        }

        ais.put(entry.getKey(), assignmentInfoList);
      };

      threadPool.execute(estimationTask);
    }

    threadPool.shutdown();

    while (!threadPool.isTerminated()) {
      try {
        threadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.error("Encountered InterruptedException while waiting for the threadPool to terminate.",
            e);
        throw new RuntimeException(e);
      }
    }

    long t2 = System.currentTimeMillis();

    log.debug(String.format("Estimated map files sizes in %6.2f secs", (t2 - t1) / 1000.0));

    return ais;
  }

  private static Map<KeyExtent,String> locationsOf(Map<Path,List<TabletLocation>> assignments) {
    Map<KeyExtent,String> result = new HashMap<>();
    for (List<TabletLocation> entry : assignments.values()) {
      for (TabletLocation tl : entry) {
        result.put(tl.tablet_extent, tl.tablet_location);
      }
    }
    return result;
  }

  private Map<Path,List<KeyExtent>> assignMapFiles(VolumeManager fs,
      Map<Path,List<TabletLocation>> assignments, Collection<Path> paths, int numThreads,
      int numMapThreads) {
    timer.start(Timers.EXAMINE_MAP_FILES);
    Map<Path,List<AssignmentInfo>> assignInfo =
        estimateSizes(fs, assignments, paths, numMapThreads);
    timer.stop(Timers.EXAMINE_MAP_FILES);

    Map<Path,List<KeyExtent>> ret;

    timer.start(Timers.IMPORT_MAP_FILES);
    ret = assignMapFiles(assignInfo, locationsOf(assignments), numThreads);
    timer.stop(Timers.IMPORT_MAP_FILES);

    return ret;
  }

  private class AssignmentTask implements Runnable {
    final Map<Path,List<KeyExtent>> assignmentFailures;
    HostAndPort location;
    private Map<KeyExtent,List<PathSize>> assignmentsPerTablet;

    public AssignmentTask(Map<Path,List<KeyExtent>> assignmentFailures, String location,
        Map<KeyExtent,List<PathSize>> assignmentsPerTablet) {
      this.assignmentFailures = assignmentFailures;
      this.location = HostAndPort.fromString(location);
      this.assignmentsPerTablet = assignmentsPerTablet;
    }

    private void handleFailures(Collection<KeyExtent> failures, String message) {
      failures.forEach(ke -> {
        List<PathSize> mapFiles = assignmentsPerTablet.get(ke);
        synchronized (assignmentFailures) {
          mapFiles.forEach(pathSize -> assignmentFailures
              .computeIfAbsent(pathSize.path, k -> new ArrayList<>()).add(ke));
        }
        log.info("Could not assign {} map files to tablet {} because : {}.  Will retry ...",
            mapFiles.size(), ke, message);
      });
    }

    @Override
    public void run() {
      HashSet<Path> uniqMapFiles = new HashSet<>();
      for (List<PathSize> mapFiles : assignmentsPerTablet.values()) {
        for (PathSize ps : mapFiles) {
          uniqMapFiles.add(ps.path);
        }
      }

      log.debug("Assigning {} map files to {} tablets at {}", uniqMapFiles.size(),
          assignmentsPerTablet.size(), location);

      try {
        List<KeyExtent> failures = assignMapFiles(context, location, assignmentsPerTablet);
        handleFailures(failures, "Not Serving Tablet");
      } catch (AccumuloException | AccumuloSecurityException e) {
        handleFailures(assignmentsPerTablet.keySet(), e.getMessage());
      }
    }

  }

  private static class PathSize {
    public PathSize(Path mapFile, long estSize) {
      this.path = mapFile;
      this.estSize = estSize;
    }

    Path path;
    long estSize;

    @Override
    public String toString() {
      return path + " " + estSize;
    }
  }

  private Map<Path,List<KeyExtent>> assignMapFiles(Map<Path,List<AssignmentInfo>> assignments,
      Map<KeyExtent,String> locations, int numThreads) {

    // group assignments by tablet
    Map<KeyExtent,List<PathSize>> assignmentsPerTablet = new TreeMap<>();
    assignments.forEach((mapFile, tabletsToAssignMapFileTo) -> tabletsToAssignMapFileTo
        .forEach(assignmentInfo -> assignmentsPerTablet
            .computeIfAbsent(assignmentInfo.ke, k -> new ArrayList<>())
            .add(new PathSize(mapFile, assignmentInfo.estSize))));

    // group assignments by tabletserver

    Map<Path,List<KeyExtent>> assignmentFailures = Collections.synchronizedMap(new TreeMap<>());

    TreeMap<String,Map<KeyExtent,List<PathSize>>> assignmentsPerTabletServer = new TreeMap<>();

    assignmentsPerTablet.forEach((ke, pathSizes) -> {
      String location = locations.get(ke);
      if (location == null) {
        synchronized (assignmentFailures) {
          pathSizes.forEach(pathSize -> assignmentFailures
              .computeIfAbsent(pathSize.path, k -> new ArrayList<>()).add(ke));
        }
        log.warn(
            "Could not assign {} map files to tablet {} because it had no location, will retry ...",
            pathSizes.size(), ke);
      } else {
        assignmentsPerTabletServer.computeIfAbsent(location, k -> new TreeMap<>()).put(ke,
            pathSizes);
      }
    });

    ExecutorService threadPool =
        ThreadPools.getServerThreadPools().createFixedThreadPool(numThreads, "submit", false);

    for (Entry<String,Map<KeyExtent,List<PathSize>>> entry : assignmentsPerTabletServer
        .entrySet()) {
      String location = entry.getKey();
      threadPool.execute(new AssignmentTask(assignmentFailures, location, entry.getValue()));
    }

    threadPool.shutdown();

    while (!threadPool.isTerminated()) {
      try {
        threadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.error(
            "Encountered InterruptedException while waiting for the thread pool to terminate.", e);
        throw new RuntimeException(e);
      }
    }

    return assignmentFailures;
  }

  private List<KeyExtent> assignMapFiles(ClientContext context, HostAndPort location,
      Map<KeyExtent,List<PathSize>> assignmentsPerTablet)
      throws AccumuloException, AccumuloSecurityException {
    try {
      long timeInMillis = context.getConfiguration().getTimeInMillis(Property.TSERV_BULK_TIMEOUT);
      TabletClientService.Iface client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location, context, timeInMillis);
      try {
        HashMap<KeyExtent,Map<String,org.apache.accumulo.core.dataImpl.thrift.MapFileInfo>> files =
            new HashMap<>();
        for (Entry<KeyExtent,List<PathSize>> entry : assignmentsPerTablet.entrySet()) {
          HashMap<String,org.apache.accumulo.core.dataImpl.thrift.MapFileInfo> tabletFiles =
              new HashMap<>();
          files.put(entry.getKey(), tabletFiles);

          for (PathSize pathSize : entry.getValue()) {
            org.apache.accumulo.core.dataImpl.thrift.MapFileInfo mfi =
                new org.apache.accumulo.core.dataImpl.thrift.MapFileInfo(pathSize.estSize);
            tabletFiles.put(pathSize.path.toString(), mfi);
          }
        }

        log.debug("Asking {} to bulk load {}", location, files);
        List<TKeyExtent> failures =
            client.bulkImport(TraceUtil.traceInfo(), context.rpcCreds(), tid,
                files.entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().toThrift(), Entry::getValue)),
                setTime);

        return failures.stream().map(KeyExtent::fromThrift).collect(Collectors.toList());
      } finally {
        ThriftUtil.returnClient((TServiceClient) client, context);
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Exception t) {
      log.error("Encountered unknown exception in assignMapFiles.", t);
      throw new AccumuloException(t);
    }
  }

  public static List<TabletLocation> findOverlappingTablets(ServerContext context, VolumeManager fs,
      TabletLocator locator, Path file, CryptoService cs) throws Exception {
    return findOverlappingTablets(context, fs, locator, file, null, null, cs);
  }

  public static List<TabletLocation> findOverlappingTablets(ServerContext context, VolumeManager fs,
      TabletLocator locator, Path file, KeyExtent failed, CryptoService cs) throws Exception {
    locator.invalidateCache(failed);
    Text start = getStartRowForExtent(failed);
    return findOverlappingTablets(context, fs, locator, file, start, failed.endRow(), cs);
  }

  protected static Text getStartRowForExtent(KeyExtent extent) {
    Text start = extent.prevEndRow();
    if (start != null) {
      start = new Text(start);
      // ACCUMULO-3967 We want the first possible key in this tablet, not the following row from the
      // previous tablet
      start.append(byte0, 0, 1);
    }
    return start;
  }

  static final byte[] byte0 = {0};

  public static List<TabletLocation> findOverlappingTablets(ServerContext context, VolumeManager vm,
      TabletLocator locator, Path file, Text startRow, Text endRow, CryptoService cs)
      throws Exception {
    List<TabletLocation> result = new ArrayList<>();
    Collection<ByteSequence> columnFamilies = Collections.emptyList();
    String filename = file.toString();
    // log.debug(filename + " finding overlapping tablets " + startRow + " -> " + endRow);
    FileSystem fs = vm.getFileSystemByPath(file);
    try (FileSKVIterator reader =
        FileOperations.getInstance().newReaderBuilder().forFile(filename, fs, fs.getConf(), cs)
            .withTableConfiguration(context.getConfiguration()).seekToBeginning().build()) {
      Text row = startRow;
      if (row == null) {
        row = new Text();
      }
      while (true) {
        // log.debug(filename + " Seeking to row " + row);
        reader.seek(new Range(row, null), columnFamilies, false);
        if (!reader.hasTop()) {
          // log.debug(filename + " not found");
          break;
        }
        row = reader.getTopKey().getRow();
        TabletLocation tabletLocation = locator.locateTablet(context, row, false, true);
        // log.debug(filename + " found row " + row + " at location " + tabletLocation);
        result.add(tabletLocation);
        row = tabletLocation.tablet_extent.endRow();
        if (row != null && (endRow == null || row.compareTo(endRow) < 0)) {
          row = new Text(row);
          row.append(byte0, 0, byte0.length);
        } else {
          break;
        }
      }
    }
    // log.debug(filename + " to be sent to " + result);
    return result;
  }

  public static class AssignmentStats {
    private Map<KeyExtent,Integer> counts;
    private int numUniqueMapFiles;
    private Map<Path,List<KeyExtent>> completeFailures = null;
    private Set<Path> failedFailures = null;

    AssignmentStats(int fileCount) {
      counts = new HashMap<>();
      numUniqueMapFiles = fileCount;
    }

    void attemptingAssignments(Map<Path,List<TabletLocation>> assignments) {
      for (Entry<Path,List<TabletLocation>> entry : assignments.entrySet()) {
        for (TabletLocation tl : entry.getValue()) {

          Integer count = getCount(tl.tablet_extent);

          counts.put(tl.tablet_extent, count + 1);
        }
      }
    }

    void assignmentsFailed(Map<Path,List<KeyExtent>> assignmentFailures) {
      for (Entry<Path,List<KeyExtent>> entry : assignmentFailures.entrySet()) {
        for (KeyExtent ke : entry.getValue()) {

          Integer count = getCount(ke);

          counts.put(ke, count - 1);
        }
      }
    }

    void assignmentsAbandoned(Map<Path,List<KeyExtent>> completeFailures) {
      this.completeFailures = completeFailures;
    }

    private Integer getCount(KeyExtent parent) {
      Integer count = counts.get(parent);

      if (count == null) {
        count = 0;
      }
      return count;
    }

    void unrecoveredMapFiles(Set<Path> failedFailures) {
      this.failedFailures = failedFailures;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      int totalAssignments = 0;
      int tabletsImportedTo = 0;

      int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

      for (Entry<KeyExtent,Integer> entry : counts.entrySet()) {
        totalAssignments += entry.getValue();
        if (entry.getValue() > 0) {
          tabletsImportedTo++;
        }

        if (entry.getValue() < min) {
          min = entry.getValue();
        }

        if (entry.getValue() > max) {
          max = entry.getValue();
        }
      }

      double stddev = 0;

      for (Entry<KeyExtent,Integer> entry : counts.entrySet()) {
        stddev += Math.pow(entry.getValue() - totalAssignments / (double) counts.size(), 2);
      }

      stddev = stddev / counts.size();
      stddev = Math.sqrt(stddev);

      Set<KeyExtent> failedTablets = new HashSet<>();
      for (List<KeyExtent> ft : completeFailures.values()) {
        failedTablets.addAll(ft);
      }

      sb.append("BULK IMPORT ASSIGNMENT STATISTICS\n");
      sb.append(String.format("# of map files            : %,10d%n", numUniqueMapFiles));
      sb.append(String.format("# map files with failures : %,10d %6.2f%s%n",
          completeFailures.size(), completeFailures.size() * 100.0 / numUniqueMapFiles, "%"));
      sb.append(String.format("# failed failed map files : %,10d %s%n", failedFailures.size(),
          failedFailures.isEmpty() ? "" : " <-- THIS IS BAD"));
      sb.append(String.format("# of tablets              : %,10d%n", counts.size()));
      sb.append(String.format("# tablets imported to     : %,10d %6.2f%s%n", tabletsImportedTo,
          tabletsImportedTo * 100.0 / counts.size(), "%"));
      sb.append(String.format("# tablets with failures   : %,10d %6.2f%s%n", failedTablets.size(),
          failedTablets.size() * 100.0 / counts.size(), "%"));
      sb.append(String.format("min map files per tablet  : %,10d%n", min));
      sb.append(String.format("max map files per tablet  : %,10d%n", max));
      sb.append(String.format("avg map files per tablet  : %,10.2f (std dev = %.2f)%n",
          totalAssignments / (double) counts.size(), stddev));
      return sb.toString();
    }
  }

}
