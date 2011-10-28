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
package org.apache.accumulo.core.util;

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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;

@SuppressWarnings("deprecation")
public class BulkImportHelper {
  private static final Logger log = Logger.getLogger(BulkImportHelper.class);
  
  private StopWatch<Timers> timer;
  
  private enum Timers {
    MOVE_MAP_FILES, EXAMINE_MAP_FILES, QUERY_METADATA, IMPORT_MAP_FILES, SLEEP, TOTAL
  }
  
  private class MapFileInfo {
    Key firstKey = new Key();
    Key lastKey = new Key();
    
    public MapFileInfo(Key firstKey, Key lastKey) {
      this.firstKey = firstKey;
      this.lastKey = lastKey;
    }
    
    public Text getFirstRow() {
      return firstKey.getRow();
    }
    
    public Text getLastRow() {
      return lastKey.getRow();
    }
  }
  
  private Instance instance;
  private AuthInfo credentials;
  private String tableName;
  
  public BulkImportHelper(Instance instance, AuthInfo credentials, String tableName) {
    this.instance = instance;
    this.credentials = credentials;
    this.tableName = tableName;
  }
  
  public AssignmentStats importDirectory(Path dir, Path failureDir, int numThreads, int numAssignThreads, boolean disableGC) throws IOException,
      AccumuloException, AccumuloSecurityException {
    
    String tableId = Tables.getNameToIdMap(instance).get(tableName);
    
    Connector conn = instance.getConnector(credentials.user, credentials.password);
    if (!conn.securityOperations().hasTablePermission(credentials.user, tableName, TablePermission.WRITE)) {
      log.error("You do not have permission to write to this table, or unable to verify ability to write");
      return null;
    }
    
    timer = new StopWatch<Timers>(Timers.class);
    
    timer.start(Timers.TOTAL);
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    FileStatus status = fs.getFileStatus(dir);
    if (status == null)
      throw new RuntimeException(dir + " does not exist");
    if (!status.isDir())
      throw new RuntimeException(dir + " is not a directory");
    dir = new Path(dir.toUri().getPath());
    
    Map<Path,MapFileInfo> mapFilesInfo = null;
    Map<KeyExtent,String> locations = null;
    SortedSet<KeyExtent> tablets = null;
    AssignmentStats assignmentStats = null;
    
    Map<Path,List<KeyExtent>> completeFailures = new TreeMap<Path,List<KeyExtent>>();
    
    if (fs.exists(failureDir)) {
      log.error(failureDir + " already exist");
      throw new RuntimeException("Directory exists: " + failureDir);
    }
    
    fs.mkdirs(failureDir);
    failureDir = new Path(failureDir.toUri().getPath());
    
    ClientService.Iface client = null;
    String lockFile = null;
    String errorMsg = "Could not move files";
    
    try {
      timer.start(Timers.MOVE_MAP_FILES);
      try {
        client = ServerClient.getConnection(instance);
        lockFile = client.prepareBulkImport(null, credentials, dir.toString(), tableName, 0.15);
      } catch (Exception ex) {
        log.error(ex, ex);
        errorMsg = ex.getMessage();
      }
      timer.stop(Timers.MOVE_MAP_FILES);
      if (lockFile == null) {
        log.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
      
      Path bulkDir = new Path(lockFile).getParent();
      
      do {
        try {
          if (mapFilesInfo == null) {
            timer.start(Timers.EXAMINE_MAP_FILES);
            mapFilesInfo = getMapFileInfo(conf, fs, bulkDir, failureDir, numThreads);
            timer.stop(Timers.EXAMINE_MAP_FILES);
          }
          
          locations = new TreeMap<KeyExtent,String>();
          tablets = new TreeSet<KeyExtent>();
          
          timer.start(Timers.QUERY_METADATA);
          MetadataTable.getEntries(instance, credentials, tableId, true, locations, tablets);
          timer.stop(Timers.QUERY_METADATA);
          
          assignmentStats = new AssignmentStats(tablets, mapFilesInfo);
          
        } catch (Throwable ioe) {
          
          timer.stopIfActive(Timers.EXAMINE_MAP_FILES);
          timer.stopIfActive(Timers.QUERY_METADATA);
          
          log.warn(ioe.getMessage() + " ... retrying ...");
          ioe.printStackTrace();
          UtilWaitThread.sleep(3000);
          
          locations = null;
          tablets = null;
        }
        
      } while (mapFilesInfo == null || locations == null || tablets == null || assignmentStats == null);
      
      Set<Entry<Path,MapFileInfo>> mapFileIter = mapFilesInfo.entrySet();
      
      Map<Path,List<KeyExtent>> assignments = new TreeMap<Path,List<KeyExtent>>();
      
      for (Entry<Path,MapFileInfo> mapFile : mapFileIter) {
        ArrayList<KeyExtent> tabletsToAssignMapFileTo = findOverlappingTablets(tableId, tablets, mapFile.getValue().getFirstRow(), mapFile.getValue()
            .getLastRow());
        
        if (tabletsToAssignMapFileTo.size() == 0)
          completeFailures.put(mapFile.getKey(), tabletsToAssignMapFileTo);
        else
          assignments.put(mapFile.getKey(), tabletsToAssignMapFileTo);
      }
      
      assignmentStats.attemptingAssignments(assignments);
      Map<Path,List<KeyExtent>> assignmentFailures = assignMapFiles(instance, conf, credentials, fs, tableId, bulkDir, assignments, locations, mapFilesInfo,
          numAssignThreads, numThreads);
      assignmentStats.assignmentsFailed(assignmentFailures);
      
      Map<Path,Integer> failureCount = new TreeMap<Path,Integer>();
      
      for (Entry<Path,List<KeyExtent>> entry : assignmentFailures.entrySet())
        failureCount.put(entry.getKey(), 1);
      
      while (assignmentFailures.size() > 0) {
        // assumption about assignment failures is that it caused by a split
        // happening
        // or a missing location
        //
        // for splits we need to find children key extents that cover the
        // same
        // key range and are contiguous (no holes, no overlap)
        
        timer.start(Timers.SLEEP);
        UtilWaitThread.sleep(4000);
        timer.stop(Timers.SLEEP);
        
        // re-acquire metadata table entries
        locations = null;
        tablets = null;
        
        log.debug("Trying to assign " + assignmentFailures.size() + " map files that previously failed on some key extents");
        
        do {
          
          try {
            
            locations = new TreeMap<KeyExtent,String>();
            tablets = new TreeSet<KeyExtent>();
            
            timer.start(Timers.QUERY_METADATA);
            MetadataTable.getEntries(instance, credentials, tableId, true, locations, tablets);
            timer.stop(Timers.QUERY_METADATA);
            
          } catch (Throwable ioe) {
            
            timer.stopIfActive(Timers.QUERY_METADATA);
            
            log.warn(ioe.getMessage() + " ... retrying ...");
            UtilWaitThread.sleep(3000);
            
            locations = null;
            tablets = null;
          }
          
        } while (locations == null || tablets == null);
        
        assignments.clear();
        
        // for failed key extents, try to find children key extents to
        // assign to
        for (Entry<Path,List<KeyExtent>> entry : assignmentFailures.entrySet()) {
          Iterator<KeyExtent> keListIter = entry.getValue().iterator();
          
          ArrayList<KeyExtent> tabletsToAssignMapFileTo = new ArrayList<KeyExtent>();
          
          while (keListIter.hasNext()) {
            KeyExtent ke = keListIter.next();
            
            SortedSet<KeyExtent> children = KeyExtent.findChildren(ke, tablets);
            
            boolean contiguous = MetadataTable.isContiguousRange(ke, children);
            
            if (contiguous) {
              if (children.size() == 1) {
                tabletsToAssignMapFileTo.add(ke);
                keListIter.remove();
              } else {
                assignmentStats.tabletSplit(ke, children);
                
                MapFileInfo mapFileRange = mapFilesInfo.get(entry.getKey());
                tabletsToAssignMapFileTo.addAll(findOverlappingTablets(tableId, children, mapFileRange.getFirstRow(), mapFileRange.getLastRow()));
                keListIter.remove();
              }
            } else
              log.warn("will retry tablet " + ke + " later it does not have contiguous children " + children);
          }
          
          if (tabletsToAssignMapFileTo.size() > 0)
            assignments.put(entry.getKey(), tabletsToAssignMapFileTo);
        }
        
        assignmentStats.attemptingAssignments(assignments);
        Map<Path,List<KeyExtent>> assignmentFailures2 = assignMapFiles(instance, conf, credentials, fs, tableId, bulkDir, assignments, locations, mapFilesInfo,
            numAssignThreads, numThreads);
        assignmentStats.assignmentsFailed(assignmentFailures2);
        
        // merge assignmentFailures2 into assignmentFailures
        for (Entry<Path,List<KeyExtent>> entry : assignmentFailures2.entrySet()) {
          assignmentFailures.get(entry.getKey()).addAll(entry.getValue());
          
          Integer fc = failureCount.get(entry.getKey());
          if (fc == null)
            fc = 0;
          
          failureCount.put(entry.getKey(), fc + 1);
        }
        
        // remove map files that have no more key extents to assign
        Iterator<Entry<Path,List<KeyExtent>>> afIter = assignmentFailures.entrySet().iterator();
        while (afIter.hasNext()) {
          Entry<Path,List<KeyExtent>> entry = afIter.next();
          if (entry.getValue().size() == 0)
            afIter.remove();
        }
        
        Set<Entry<Path,Integer>> failureIter = failureCount.entrySet();
        for (Entry<Path,Integer> entry : failureIter) {
          if (entry.getValue() > 3 && assignmentFailures.get(entry.getKey()) != null) {
            log.error("Map file " + entry.getKey() + " failed more than three times, giving up.");
            completeFailures.put(entry.getKey(), assignmentFailures.get(entry.getKey()));
            assignmentFailures.remove(entry.getKey());
          }
        }
        
      }
      
      assignmentStats.assignmentsAbandoned(completeFailures);
      Set<Path> failedFailures = processFailures(conf, fs, failureDir, completeFailures);
      assignmentStats.unrecoveredMapFiles(failedFailures);
      
      try {
        client.finishBulkImport(null, credentials, tableName, lockFile, disableGC);
      } catch (Exception ex) {
        log.error("Unable to finalize bulk import", ex);
      }
      
      timer.stop(Timers.TOTAL);
      printReport();
      return assignmentStats;
    } finally {
      if (client != null)
        ServerClient.close(client);
    }
  }
  
  private void printReport() {
    long totalTime = 0;
    for (Timers t : Timers.values()) {
      if (t == Timers.TOTAL)
        continue;
      
      totalTime += timer.get(t);
    }
    
    log.debug("BULK IMPORT TIMING STATISTICS");
    log.debug(String.format("Move map files       : %,10.2f secs %6.2f%s", timer.getSecs(Timers.MOVE_MAP_FILES), 100.0 * timer.get(Timers.MOVE_MAP_FILES)
        / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Examine map files    : %,10.2f secs %6.2f%s", timer.getSecs(Timers.EXAMINE_MAP_FILES), 100.0 * timer.get(Timers.EXAMINE_MAP_FILES)
        / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Query %-14s : %,10.2f secs %6.2f%s", Constants.METADATA_TABLE_NAME, timer.getSecs(Timers.QUERY_METADATA),
        100.0 * timer.get(Timers.QUERY_METADATA) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Import Map Files     : %,10.2f secs %6.2f%s", timer.getSecs(Timers.IMPORT_MAP_FILES), 100.0 * timer.get(Timers.IMPORT_MAP_FILES)
        / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Sleep                : %,10.2f secs %6.2f%s", timer.getSecs(Timers.SLEEP),
        100.0 * timer.get(Timers.SLEEP) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Misc                 : %,10.2f secs %6.2f%s", (timer.get(Timers.TOTAL) - totalTime) / 1000.0, 100.0
        * (timer.get(Timers.TOTAL) - totalTime) / timer.get(Timers.TOTAL), "%"));
    log.debug(String.format("Total                : %,10.2f secs", timer.getSecs(Timers.TOTAL)));
  }
  
  private Set<Path> processFailures(Configuration conf, FileSystem fs, Path failureDir, Map<Path,List<KeyExtent>> completeFailures) {
    // we should check if map file was not assigned to any tablets, then we
    // should just move it; not currently being done?
    
    Set<Entry<Path,List<KeyExtent>>> es = completeFailures.entrySet();
    
    if (completeFailures.size() == 0)
      return Collections.emptySet();
    
    log.error("The following map files failed completely, saving this info to : " + new Path(failureDir, "failures.seq"));
    
    for (Entry<Path,List<KeyExtent>> entry : es) {
      List<KeyExtent> extents = entry.getValue();
      
      for (KeyExtent keyExtent : extents)
        log.error("\t" + entry.getKey() + " -> " + keyExtent);
    }
    
    try {
      
      Writer outSeq = SequenceFile.createWriter(fs, conf, new Path(failureDir, "failures.seq"), Text.class, KeyExtent.class);
      
      for (Entry<Path,List<KeyExtent>> entry : es) {
        List<KeyExtent> extents = entry.getValue();
        
        for (KeyExtent keyExtent : extents)
          outSeq.append(new Text(entry.getKey().toString()), keyExtent);
      }
      
      outSeq.close();
    } catch (IOException ioe) {
      log.error("Failed to create " + new Path(failureDir, "failures.seq") + " : " + ioe.getMessage());
    }
    
    // we should make copying multithreaded
    
    Set<Path> failedCopies = new HashSet<Path>();
    
    for (Entry<Path,List<KeyExtent>> entry : es) {
      Path dest = new Path(failureDir, entry.getKey().getName());
      
      log.debug("Copying " + entry.getKey() + " to " + dest);
      
      try {
        org.apache.hadoop.fs.FileUtil.copy(fs, entry.getKey(), fs, dest, false, conf);
      } catch (IOException ioe) {
        log.error("Failed to copy " + entry.getKey() + " : " + ioe.getMessage());
        failedCopies.add(entry.getKey());
      }
    }
    
    return failedCopies;
  }
  
  private class AssignmentInfo {
    public AssignmentInfo(KeyExtent keyExtent, Long estSize) {
      this.ke = keyExtent;
      this.estSize = estSize;
    }
    
    KeyExtent ke;
    long estSize;
  }
  
  private Map<Path,List<AssignmentInfo>> estimateSizes(final Configuration conf, final FileSystem fs, Path bulkDir, Map<Path,List<KeyExtent>> assignments,
      Map<Path,MapFileInfo> mapFilesInfo, int numThreads) {
    
    long t1 = System.currentTimeMillis();
    final Map<Path,Long> mapFileSizes = new TreeMap<Path,Long>();
    
    try {
      for (FileStatus fileStatus : fs.globStatus(new Path(bulkDir.toString() + "/[0-9]*")))
        if (!fileStatus.getPath().getName().endsWith("." + MyMapFile.EXTENSION))
          mapFileSizes.put(fileStatus.getPath(), fileStatus.getLen());
      
      for (FileStatus fileStatus : fs.globStatus(new Path(bulkDir.toString() + "/[0-9]*." + MyMapFile.EXTENSION + "/" + MyMapFile.DATA_FILE_NAME)))
        mapFileSizes.put(fileStatus.getPath().getParent(), fileStatus.getLen());
    } catch (IOException e) {
      log.error("Failed to list map files in " + bulkDir + " " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    
    final Map<Path,List<AssignmentInfo>> ais = Collections.synchronizedMap(new TreeMap<Path,List<AssignmentInfo>>());
    
    ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    
    for (final Entry<Path,List<KeyExtent>> entry : assignments.entrySet()) {
      if (entry.getValue().size() == 1) {
        MapFileInfo mapFileRange = mapFilesInfo.get(entry.getKey());
        KeyExtent tabletExtent = entry.getValue().get(0);
        
        // if the tablet completely contains the map file, there is no
        // need to estimate its
        // size
        
        if (tabletExtent.contains(mapFileRange.getFirstRow()) && tabletExtent.contains(mapFileRange.getLastRow())) {
          ais.put(entry.getKey(), Collections.singletonList(new AssignmentInfo(tabletExtent, mapFileSizes.get(entry.getKey()))));
          continue;
        }
      }
      
      Runnable estimationTask = new Runnable() {
        public void run() {
          Map<KeyExtent,Long> estimatedSizes = null;
          
          try {
            estimatedSizes = FileUtil.estimateSizes(entry.getKey(), mapFileSizes.get(entry.getKey()), entry.getValue(), conf, fs);
          } catch (IOException e) {
            log.warn("Failed to estimate map file sizes " + e.getMessage());
          }
          
          if (estimatedSizes == null) {
            // estimation failed, do a simple estimation
            estimatedSizes = new TreeMap<KeyExtent,Long>();
            long estSize = (long) (mapFileSizes.get(entry.getKey()) / (double) entry.getValue().size());
            for (KeyExtent ke : entry.getValue())
              estimatedSizes.put(ke, estSize);
          }
          
          List<AssignmentInfo> assignmentInfoList = new ArrayList<AssignmentInfo>(estimatedSizes.size());
          
          for (Entry<KeyExtent,Long> entry2 : estimatedSizes.entrySet())
            assignmentInfoList.add(new AssignmentInfo(entry2.getKey(), entry2.getValue()));
          
          ais.put(entry.getKey(), assignmentInfoList);
        }
      };
      
      threadPool.submit(estimationTask);
    }
    
    threadPool.shutdown();
    
    while (!threadPool.isTerminated()) {
      try {
        threadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    
    long t2 = System.currentTimeMillis();
    
    log.debug(String.format("Estimated map files sizes in %6.2f secs", (t2 - t1) / 1000.0));
    
    return ais;
  }
  
  private Map<Path,List<KeyExtent>> assignMapFiles(Instance instance, Configuration conf, AuthInfo credentials, FileSystem fs, String tableName, Path bulkDir,
      Map<Path,List<KeyExtent>> assignments, Map<KeyExtent,String> locations, Map<Path,MapFileInfo> mapFilesInfo, int numThreads, int numMapThreads) {
    timer.start(Timers.EXAMINE_MAP_FILES);
    Map<Path,List<AssignmentInfo>> assignInfo = estimateSizes(conf, fs, bulkDir, assignments, mapFilesInfo, numMapThreads);
    timer.stop(Timers.EXAMINE_MAP_FILES);
    
    Map<Path,List<KeyExtent>> ret;
    
    timer.start(Timers.IMPORT_MAP_FILES);
    ret = assignMapFiles(credentials, tableName, assignInfo, locations, mapFilesInfo, numThreads);
    timer.stop(Timers.IMPORT_MAP_FILES);
    
    return ret;
  }
  
  private class AssignmentTask implements Runnable {
    Map<Path,List<KeyExtent>> assignmentFailures;
    String location;
    AuthInfo credentials;
    private Map<KeyExtent,List<PathSize>> assignmentsPerTablet;
    
    public AssignmentTask(AuthInfo credentials, Map<Path,List<KeyExtent>> assignmentFailures, String tableName, String location,
        Map<KeyExtent,List<PathSize>> assignmentsPerTablet) {
      this.assignmentFailures = assignmentFailures;
      this.location = location;
      this.assignmentsPerTablet = assignmentsPerTablet;
      this.credentials = credentials;
    }
    
    private void handleFailures(Collection<KeyExtent> failures, String message) {
      for (KeyExtent ke : failures) {
        List<PathSize> mapFiles = assignmentsPerTablet.get(ke);
        synchronized (assignmentFailures) {
          for (PathSize pathSize : mapFiles) {
            List<KeyExtent> existingFailures = assignmentFailures.get(pathSize.path);
            if (existingFailures == null) {
              existingFailures = new ArrayList<KeyExtent>();
              assignmentFailures.put(pathSize.path, existingFailures);
            }
            
            existingFailures.add(ke);
          }
        }
        
        log.warn("Could not assign  " + mapFiles.size() + " map files to tablet " + ke + " because : " + message + ".  Will retry ...");
      }
    }
    
    public void run() {
      HashSet<Path> uniqMapFiles = new HashSet<Path>();
      for (List<PathSize> mapFiles : assignmentsPerTablet.values())
        for (PathSize ps : mapFiles)
          uniqMapFiles.add(ps.path);
      
      log.debug("Assigning " + uniqMapFiles.size() + " map files to " + assignmentsPerTablet.size() + " tablets at " + location);
      
      try {
        List<KeyExtent> failures = assignMapFiles(credentials, location, assignmentsPerTablet);
        handleFailures(failures, "Not Serving Tablet");
      } catch (AccumuloException e) {
        handleFailures(assignmentsPerTablet.keySet(), e.getMessage());
      } catch (AccumuloSecurityException e) {
        handleFailures(assignmentsPerTablet.keySet(), e.getMessage());
      }
    }
    
  }
  
  private class PathSize {
    public PathSize(Path mapFile, long estSize, Text firstRow, Text lastRow) {
      this.path = mapFile;
      this.estSize = estSize;
      this.firstRow = firstRow;
      this.lastRow = lastRow;
    }
    
    Path path;
    long estSize;
    Text firstRow;
    Text lastRow;
    
    public String toString() {
      return path + " " + estSize + " " + firstRow + " " + lastRow;
    }
  }
  
  private Map<Path,List<KeyExtent>> assignMapFiles(AuthInfo credentials, String tableName, Map<Path,List<AssignmentInfo>> assignments,
      Map<KeyExtent,String> locations, Map<Path,MapFileInfo> mapFilesInfo, int numThreads) {
    
    // group assignments by tablet
    Map<KeyExtent,List<PathSize>> assignmentsPerTablet = new TreeMap<KeyExtent,List<PathSize>>();
    for (Entry<Path,List<AssignmentInfo>> entry : assignments.entrySet()) {
      Path mapFile = entry.getKey();
      List<AssignmentInfo> tabletsToAssignMapFileTo = entry.getValue();
      
      for (AssignmentInfo ai : tabletsToAssignMapFileTo) {
        List<PathSize> mapFiles = assignmentsPerTablet.get(ai.ke);
        if (mapFiles == null) {
          mapFiles = new ArrayList<PathSize>();
          assignmentsPerTablet.put(ai.ke, mapFiles);
        }
        
        mapFiles.add(new PathSize(mapFile, ai.estSize, mapFilesInfo.get(mapFile).getFirstRow(), mapFilesInfo.get(mapFile).getLastRow()));
      }
    }
    
    // group assignments by tabletserver
    
    Map<Path,List<KeyExtent>> assignmentFailures = Collections.synchronizedMap(new TreeMap<Path,List<KeyExtent>>());
    
    TreeMap<String,Map<KeyExtent,List<PathSize>>> assignmentsPerTabletServer = new TreeMap<String,Map<KeyExtent,List<PathSize>>>();
    
    for (Entry<KeyExtent,List<PathSize>> entry : assignmentsPerTablet.entrySet()) {
      KeyExtent ke = entry.getKey();
      String location = locations.get(ke);
      
      if (location == null) {
        for (PathSize pathSize : entry.getValue()) {
          synchronized (assignmentFailures) {
            List<KeyExtent> failures = assignmentFailures.get(pathSize.path);
            if (failures == null) {
              failures = new ArrayList<KeyExtent>();
              assignmentFailures.put(pathSize.path, failures);
            }
            
            failures.add(ke);
          }
        }
        
        log.warn("Could not assign " + entry.getValue().size() + " map files to tablet " + ke + " because it had no location, will retry ...");
        
        continue;
      }
      
      Map<KeyExtent,List<PathSize>> apt = assignmentsPerTabletServer.get(location);
      if (apt == null) {
        apt = new TreeMap<KeyExtent,List<PathSize>>();
        assignmentsPerTabletServer.put(location, apt);
      }
      
      apt.put(entry.getKey(), entry.getValue());
    }
    
    ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    
    for (Entry<String,Map<KeyExtent,List<PathSize>>> entry : assignmentsPerTabletServer.entrySet()) {
      String location = entry.getKey();
      threadPool.submit(new AssignmentTask(credentials, assignmentFailures, tableName, location, entry.getValue()));
    }
    
    threadPool.shutdown();
    
    while (!threadPool.isTerminated()) {
      try {
        threadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    
    return assignmentFailures;
  }
  
  private List<KeyExtent> assignMapFiles(AuthInfo credentials, String location, Map<KeyExtent,List<PathSize>> assignmentsPerTablet) throws AccumuloException,
      AccumuloSecurityException {
    try {
      TabletClientService.Iface client = ThriftUtil.getTServerClient(location, instance.getConfiguration());
      try {
        HashMap<KeyExtent,Map<String,org.apache.accumulo.core.data.thrift.MapFileInfo>> files = new HashMap<KeyExtent,Map<String,org.apache.accumulo.core.data.thrift.MapFileInfo>>();
        for (Entry<KeyExtent,List<PathSize>> entry : assignmentsPerTablet.entrySet()) {
          HashMap<String,org.apache.accumulo.core.data.thrift.MapFileInfo> tabletFiles = new HashMap<String,org.apache.accumulo.core.data.thrift.MapFileInfo>();
          files.put(entry.getKey(), tabletFiles);
          
          for (PathSize pathSize : entry.getValue()) {
            org.apache.accumulo.core.data.thrift.MapFileInfo mfi = new org.apache.accumulo.core.data.thrift.MapFileInfo(pathSize.estSize);
            tabletFiles.put(pathSize.path.toUri().getPath().toString(), mfi);
          }
        }
        
        List<TKeyExtent> failures = client.bulkImport(null, credentials, Translator.translate(files, Translator.KET));
        
        return Translator.translate(failures, Translator.TKET);
      } finally {
        ThriftUtil.returnClient((TServiceClient) client);
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (Throwable t) {
      t.printStackTrace();
      throw new AccumuloException(t);
    }
  }
  
  private ArrayList<KeyExtent> findOverlappingTablets(String tableName, SortedSet<KeyExtent> tablets, Text firstRow, Text lastRow) {
    SortedSet<KeyExtent> tailMap = tablets.tailSet(new KeyExtent(new Text(tableName), firstRow, null));
    Iterator<KeyExtent> esIter = tailMap.iterator();
    
    Text mapFileLastRow = lastRow;
    
    ArrayList<KeyExtent> tabletsToAssignMapFileTo = new ArrayList<KeyExtent>();
    
    // find all tablets a map file should be assigned to
    while (esIter.hasNext()) {
      KeyExtent ke = esIter.next();
      
      Text tabletPrevEndRow = ke.getPrevEndRow();
      
      if (tabletPrevEndRow != null && tabletPrevEndRow.compareTo(mapFileLastRow) >= 0)
        break;
      
      tabletsToAssignMapFileTo.add(ke);
    }
    
    return tabletsToAssignMapFileTo;
  }
  
  public static class AssignmentStats {
    private Map<KeyExtent,Integer> counts;
    private int numUniqueMapFiles;
    private Map<Path,List<KeyExtent>> completeFailures = null;
    private Set<Path> failedFailures = null;
    
    AssignmentStats(SortedSet<KeyExtent> tablets, Map<Path,MapFileInfo> mapFilesInfo) {
      counts = new HashMap<KeyExtent,Integer>();
      for (KeyExtent keyExtent : tablets)
        counts.put(keyExtent, 0);
      
      numUniqueMapFiles = mapFilesInfo.size();
    }
    
    void attemptingAssignments(Map<Path,List<KeyExtent>> assignments) {
      for (Entry<Path,List<KeyExtent>> entry : assignments.entrySet()) {
        for (KeyExtent ke : entry.getValue()) {
          
          Integer count = getCount(ke);
          
          counts.put(ke, count + 1);
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
    
    void tabletSplit(KeyExtent parent, Collection<KeyExtent> children) {
      Integer count = getCount(parent);
      
      counts.remove(parent);
      
      for (KeyExtent keyExtent : children)
        counts.put(keyExtent, count);
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
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      int totalAssignments = 0;
      int tabletsImportedTo = 0;
      
      int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
      
      for (Entry<KeyExtent,Integer> entry : counts.entrySet()) {
        totalAssignments += entry.getValue();
        if (entry.getValue() > 0)
          tabletsImportedTo++;
        
        if (entry.getValue() < min)
          min = entry.getValue();
        
        if (entry.getValue() > max)
          max = entry.getValue();
      }
      
      double stddev = 0;
      
      for (Entry<KeyExtent,Integer> entry : counts.entrySet())
        stddev += Math.pow(entry.getValue() - totalAssignments / (double) counts.size(), 2);
      
      stddev = stddev / counts.size();
      stddev = Math.sqrt(stddev);
      
      Set<KeyExtent> failedTablets = new HashSet<KeyExtent>();
      for (List<KeyExtent> ft : completeFailures.values())
        failedTablets.addAll(ft);
      
      sb.append("BULK IMPORT ASSIGNMENT STATISTICS\n");
      sb.append(String.format("# of map files            : %,10d\n", numUniqueMapFiles));
      sb.append(String.format("# map files with failures : %,10d %6.2f%s\n", completeFailures.size(), completeFailures.size() * 100.0 / numUniqueMapFiles, "%"));
      sb.append(String.format("# failed failed map files : %,10d %s\n", failedFailures.size(), failedFailures.size() > 0 ? " <-- THIS IS BAD" : ""));
      sb.append(String.format("# of tablets              : %,10d\n", counts.size()));
      sb.append(String.format("# tablets imported to     : %,10d %6.2f%s\n", tabletsImportedTo, tabletsImportedTo * 100.0 / counts.size(), "%"));
      sb.append(String.format("# tablets with failures   : %,10d %6.2f%s\n", failedTablets.size(), failedTablets.size() * 100.0 / counts.size(), "%"));
      sb.append(String.format("min map files per tablet  : %,10d\n", min));
      sb.append(String.format("max map files per tablet  : %,10d\n", max));
      sb.append(String.format("avg map files per tablet  : %,10.2f (std dev = %.2f)\n", totalAssignments / (double) counts.size(), stddev));
      return sb.toString();
    }
  }
  
  private Map<Path,MapFileInfo> getMapFileInfo(final Configuration conf, final FileSystem fs, Path bulkDir, final Path failureDir, int numThreads)
      throws IOException {
    Map<Path,MapFileInfo> mapFilesInfo = new TreeMap<Path,MapFileInfo>();
    final Map<Path,MapFileInfo> synchronizedMapFilesInfo = Collections.synchronizedMap(mapFilesInfo);
    
    FileStatus[] files = fs.globStatus(new Path(bulkDir.toString() + "/[0-9]*"));
    
    ExecutorService tp = Executors.newFixedThreadPool(numThreads);
    
    for (final FileStatus file : files) {
      Runnable r = new Runnable() {
        public void run() {
          boolean successful;
          int failures = 0;
          
          do {
            successful = true;
            FileSKVIterator reader = null;
            try {
              reader = FileOperations.getInstance().openReader(file.getPath().toString(), false, fs, conf, instance.getConfiguration());
              
              Key firstKey = reader.getFirstKey();
              if (firstKey != null) {
                synchronizedMapFilesInfo.put(file.getPath(), new MapFileInfo(firstKey, reader.getLastKey()));
              } else {
                log.warn(file.getPath() + " is an empty map file, assigning it to first tablet");
                // this is a hack... the file is empty, it has already been moved by a tserver, so it can not be deleted
                // by the client now... if the file is not deleted here, it will never be removed because it is not referenced
                // so ask 1st tablet to import it, eventually it will be deleted... this is the best option w/o changing
                // the thrift API.. if all empty files go to one tablet, more likely to be compacted away quickly
                synchronizedMapFilesInfo.put(file.getPath(), new MapFileInfo(new Key(), new Key()));
              }
              
            } catch (IOException ioe) {
              successful = false;
              failures++;
              log.warn("Failed to read map file " + file.getPath() + " [" + ioe.getMessage() + "] failures = " + failures);
              if (failures < 3)
                try {
                  Thread.sleep(500);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
            } catch (Throwable t) {
              successful = false;
              failures = 3;
              log.warn("Failed to read map file " + file.getPath(), t);
            } finally {
              if (reader != null)
                try {
                  reader.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
            }
          } while (!successful && failures < 3);
          
          if (!successful) {
            Path dest = new Path(failureDir, file.getPath().getName());
            log.error("Failed to read map file " + file.getPath() + ", moving map file to " + dest);
            try {
              fs.rename(file.getPath(), dest);
            } catch (IOException e) {
              log.error("Failed to move map file from " + file.getPath() + " to " + dest + " [" + e.getMessage() + "] it could be garbage collected");
            }
          }
        }
        
      };
      
      tp.submit(r);
    }
    
    tp.shutdown();
    
    while (!tp.isTerminated())
      try {
        tp.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    
    return mapFilesInfo;
  }
}
