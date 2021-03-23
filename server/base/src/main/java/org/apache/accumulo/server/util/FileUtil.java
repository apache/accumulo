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
package org.apache.accumulo.server.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedMap;

public class FileUtil {

  public static class FileInfo {
    Key firstKey = new Key();
    Key lastKey = new Key();

    public FileInfo(Key firstKey, Key lastKey) {
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

  private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

  private static Path createTmpDir(ServerContext context, String tabletDirectory)
      throws IOException {

    VolumeManager fs = context.getVolumeManager();

    Path result = null;
    while (result == null) {
      result = new Path(tabletDirectory + Path.SEPARATOR + "tmp/idxReduce_"
          + String.format("%09d", new SecureRandom().nextInt(Integer.MAX_VALUE)));
      try {
        fs.getFileStatus(result);
        result = null;
        continue;
      } catch (FileNotFoundException fne) {
        // found an unused temp directory
      }

      fs.mkdirs(result);

      // try to reserve the tmp dir
      // In some versions of hadoop, two clients concurrently trying to create the same directory
      // might both return true
      // Creating a file is not subject to this, so create a special file to make sure we solely
      // will use this directory
      if (!fs.createNewFile(new Path(result, "__reserve")))
        result = null;
    }
    return result;
  }

  public static Collection<TabletFile> reduceFiles(ServerContext context,
      TableConfiguration tableConf, Text prevEndRow, Text endRow, Collection<TabletFile> mapFiles,
      int maxFiles, Path tmpDir, int pass) throws IOException {

    ArrayList<TabletFile> paths = new ArrayList<>(mapFiles);

    if (paths.size() <= maxFiles)
      return paths;

    String newDir = String.format("%s/pass_%04d", tmpDir, pass);

    int start = 0;

    ArrayList<TabletFile> outFiles = new ArrayList<>();

    int count = 0;

    while (start < paths.size()) {
      int end = Math.min(maxFiles + start, paths.size());
      List<TabletFile> inFiles = paths.subList(start, end);

      start = end;

      TabletFile newMapFile =
          new TabletFile(new Path(String.format("%s/%04d.%s", newDir, count++, RFile.EXTENSION)));

      outFiles.add(newMapFile);
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(newMapFile.getPath());
      FileSKVWriter writer = new RFileOperations().newWriterBuilder()
          .forFile(newMapFile.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
          .build();
      writer.startDefaultLocalityGroup();
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(inFiles.size());

      FileSKVIterator reader = null;
      try {
        for (TabletFile file : inFiles) {
          ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
          reader = FileOperations.getInstance().newIndexReaderBuilder()
              .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
              .decrypt(tableConf.getDecrypters()).build();
          iters.add(reader);
        }

        MultiIterator mmfi = new MultiIterator(iters, true);

        while (mmfi.hasTop()) {
          Key key = mmfi.getTopKey();

          boolean gtPrevEndRow = prevEndRow == null || key.compareRow(prevEndRow) > 0;
          boolean lteEndRow = endRow == null || key.compareRow(endRow) <= 0;

          if (gtPrevEndRow && lteEndRow)
            writer.append(key, new Value(new byte[0]));

          if (!lteEndRow)
            break;

          mmfi.next();
        }
      } finally {
        try {
          if (reader != null)
            reader.close();
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }

        for (SortedKeyValueIterator<Key,Value> r : iters)
          try {
            if (r != null)
              ((FileSKVIterator) r).close();
          } catch (IOException e) {
            // continue closing
            log.error("{}", e.getMessage(), e);
          }

        try {
          writer.close();
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
          throw e;
        }
      }
    }

    return reduceFiles(context, tableConf, prevEndRow, endRow, outFiles, maxFiles, tmpDir,
        pass + 1);
  }

  public static double estimatePercentageLTE(ServerContext context, TableConfiguration tableConf,
      String tabletDir, Text prevEndRow, Text endRow, Collection<TabletFile> mapFiles,
      Text splitRow) throws IOException {

    Path tmpDir = null;

    int maxToOpen =
        context.getConfiguration().getCount(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN);
    ArrayList<FileSKVIterator> readers = new ArrayList<>(mapFiles.size());

    try {
      if (mapFiles.size() > maxToOpen) {
        tmpDir = createTmpDir(context, tabletDir);

        log.debug("Too many indexes ({}) to open at once for {} {}, reducing in tmpDir = {}",
            mapFiles.size(), endRow, prevEndRow, tmpDir);

        long t1 = System.currentTimeMillis();
        mapFiles =
            reduceFiles(context, tableConf, prevEndRow, endRow, mapFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();

        log.debug("Finished reducing indexes for {} {} in {}", endRow, prevEndRow,
            String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }

      if (prevEndRow == null)
        prevEndRow = new Text();

      long numKeys = 0;

      numKeys = countIndexEntries(context, tableConf, prevEndRow, endRow, mapFiles, true, readers);

      if (numKeys == 0) {
        // not enough info in the index to answer the question, so instead of going to
        // the data just punt and return .5
        return .5;
      }

      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);

      // skip the prevEndRow
      while (mmfi.hasTop() && mmfi.getTopKey().compareRow(prevEndRow) <= 0) {
        mmfi.next();
      }

      int numLte = 0;

      while (mmfi.hasTop() && mmfi.getTopKey().compareRow(splitRow) <= 0) {
        numLte++;
        mmfi.next();
      }

      if (numLte > numKeys) {
        // something went wrong
        throw new RuntimeException("numLte > numKeys " + numLte + " " + numKeys + " " + prevEndRow
            + " " + endRow + " " + splitRow + " " + mapFiles);
      }

      // do not want to return 0% or 100%, so add 1 and 2 below
      return (numLte + 1) / (double) (numKeys + 2);

    } finally {
      cleanupIndexOp(tmpDir, context.getVolumeManager(), readers);
    }
  }

  /**
   *
   * @param mapFiles
   *          - list MapFiles to find the mid point key
   *
   *          ISSUES : This method used the index files to find the mid point. If the map files have
   *          different index intervals this method will not return an accurate mid point. Also, it
   *          would be tricky to use this method in conjunction with an in memory map because the
   *          indexing interval is unknown.
   */
  public static SortedMap<Double,Key> findMidPoint(ServerContext context,
      TableConfiguration tableConf, String tabletDirectory, Text prevEndRow, Text endRow,
      Collection<TabletFile> mapFiles, double minSplit, boolean useIndex) throws IOException {

    Collection<TabletFile> origMapFiles = mapFiles;

    Path tmpDir = null;

    int maxToOpen =
        context.getConfiguration().getCount(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN);
    ArrayList<FileSKVIterator> readers = new ArrayList<>(mapFiles.size());

    try {
      if (mapFiles.size() > maxToOpen) {
        if (!useIndex)
          throw new IOException(
              "Cannot find mid point using data files, too many " + mapFiles.size());
        tmpDir = createTmpDir(context, tabletDirectory);

        log.debug("Too many indexes ({}) to open at once for {} {}, reducing in tmpDir = {}",
            mapFiles.size(), endRow, prevEndRow, tmpDir);

        long t1 = System.currentTimeMillis();
        mapFiles =
            reduceFiles(context, tableConf, prevEndRow, endRow, mapFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();

        log.debug("Finished reducing indexes for {} {} in {}", endRow, prevEndRow,
            String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }

      if (prevEndRow == null)
        prevEndRow = new Text();

      long t1 = System.currentTimeMillis();

      long numKeys = 0;

      numKeys = countIndexEntries(context, tableConf, prevEndRow, endRow, mapFiles,
          tmpDir == null ? useIndex : false, readers);

      if (numKeys == 0) {
        if (useIndex) {
          log.warn(
              "Failed to find mid point using indexes, falling back to"
                  + " data files which is slower. No entries between {} and {} for {}",
              prevEndRow, endRow, mapFiles);
          // need to pass original map files, not possibly reduced indexes
          return findMidPoint(context, tableConf, tabletDirectory, prevEndRow, endRow, origMapFiles,
              minSplit, false);
        }
        return ImmutableSortedMap.of();
      }

      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);

      // skip the prevEndRow
      while (mmfi.hasTop() && mmfi.getTopKey().compareRow(prevEndRow) <= 0)
        mmfi.next();

      // read half of the keys in the index
      TreeMap<Double,Key> ret = new TreeMap<>();
      Key lastKey = null;
      long keysRead = 0;

      Key keyBeforeMidPoint = null;
      long keyBeforeMidPointPosition = 0;

      while (keysRead < numKeys / 2) {
        if (lastKey != null && !lastKey.equals(mmfi.getTopKey(), PartialKey.ROW)
            && (keysRead - 1) / (double) numKeys >= minSplit) {
          keyBeforeMidPoint = new Key(lastKey);
          keyBeforeMidPointPosition = keysRead - 1;
        }

        if (lastKey == null)
          lastKey = new Key();

        lastKey.set(mmfi.getTopKey());

        keysRead++;

        // consume minimum
        mmfi.next();
      }

      if (keyBeforeMidPoint != null)
        ret.put(keyBeforeMidPointPosition / (double) numKeys, keyBeforeMidPoint);

      long t2 = System.currentTimeMillis();

      log.debug(
          String.format("Found midPoint from indexes in %6.2f secs.%n", ((t2 - t1) / 1000.0)));

      ret.put(.5, mmfi.getTopKey());

      // sanity check
      for (Key key : ret.values()) {
        boolean inRange =
            (key.compareRow(prevEndRow) > 0 && (endRow == null || key.compareRow(endRow) < 1));
        if (!inRange) {
          throw new IOException("Found mid point is not in range " + key + " " + prevEndRow + " "
              + endRow + " " + mapFiles);
        }
      }

      return ret;
    } finally {
      cleanupIndexOp(tmpDir, context.getVolumeManager(), readers);
    }
  }

  protected static void cleanupIndexOp(Path tmpDir, VolumeManager fs,
      ArrayList<FileSKVIterator> readers) throws IOException {
    // close all of the index sequence files
    for (FileSKVIterator r : readers) {
      try {
        if (r != null)
          r.close();
      } catch (IOException e) {
        // okay, try to close the rest anyway
        log.error("{}", e.getMessage(), e);
      }
    }

    if (tmpDir != null) {
      FileSystem actualFs = fs.getFileSystemByPath(tmpDir);
      if (actualFs.exists(tmpDir)) {
        fs.deleteRecursively(tmpDir);
        return;
      }

      log.error("Did not delete tmp dir because it wasn't a tmp dir {}", tmpDir);
    }
  }

  private static long countIndexEntries(ServerContext context, TableConfiguration tableConf,
      Text prevEndRow, Text endRow, Collection<TabletFile> mapFiles, boolean useIndex,
      ArrayList<FileSKVIterator> readers) throws IOException {
    long numKeys = 0;

    // count the total number of index entries
    for (TabletFile file : mapFiles) {
      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
      try {
        if (useIndex)
          reader = FileOperations.getInstance().newIndexReaderBuilder()
              .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
              .decrypt(tableConf.getDecrypters()).build();
        else
          reader = FileOperations.getInstance().newScanReaderBuilder()
              .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
              .decrypt(tableConf.getDecrypters())
              .overRange(new Range(prevEndRow, false, null, true), LocalityGroupUtil.EMPTY_CF_SET,
                  false)
              .build();

        while (reader.hasTop()) {
          Key key = reader.getTopKey();
          if (endRow != null && key.compareRow(endRow) > 0)
            break;
          else if (prevEndRow == null || key.compareRow(prevEndRow) > 0)
            numKeys++;

          reader.next();
        }
      } finally {
        try {
          if (reader != null)
            reader.close();
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }
      }

      if (useIndex)
        readers.add(FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
            .decrypt(tableConf.getDecrypters()).build());
      else
        readers
            .add(
                FileOperations.getInstance().newScanReaderBuilder()
                    .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
                    .overRange(new Range(prevEndRow, false, null, true),
                        LocalityGroupUtil.EMPTY_CF_SET, false)
                    .decrypt(tableConf.getDecrypters()).build());

    }
    return numKeys;
  }

  public static Map<TabletFile,FileInfo> tryToGetFirstAndLastRows(ServerContext context,
      TableConfiguration tableConf, Set<TabletFile> mapfiles) {

    HashMap<TabletFile,FileInfo> mapFilesInfo = new HashMap<>();

    long t1 = System.currentTimeMillis();

    for (TabletFile mapfile : mapfiles) {

      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(mapfile.getPath());
      try {
        reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(mapfile.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
            .decrypt(tableConf.getDecrypters()).build();

        Key firstKey = reader.getFirstKey();
        if (firstKey != null) {
          mapFilesInfo.put(mapfile, new FileInfo(firstKey, reader.getLastKey()));
        }

      } catch (IOException ioe) {
        log.warn("Failed to read map file to determine first and last key : " + mapfile, ioe);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ioe) {
            log.warn("failed to close " + mapfile, ioe);
          }
        }
      }

    }

    long t2 = System.currentTimeMillis();

    log.debug(String.format("Found first and last keys for %d map files in %6.2f secs",
        mapfiles.size(), (t2 - t1) / 1000.0));

    return mapFilesInfo;
  }

  public static WritableComparable<Key> findLastKey(ServerContext context,
      TableConfiguration tableConf, Collection<TabletFile> mapFiles) throws IOException {

    Key lastKey = null;

    for (TabletFile file : mapFiles) {
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(file.getPathStr(), ns, ns.getConf()).withTableConfiguration(tableConf)
          .decrypt(tableConf.getDecrypters()).seekToBeginning().build();

      try {
        if (!reader.hasTop())
          // file is empty, so there is no last key
          continue;

        Key key = reader.getLastKey();

        if (lastKey == null || key.compareTo(lastKey) > 0)
          lastKey = key;
      } finally {
        try {
          if (reader != null)
            reader.close();
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }
      }
    }

    return lastKey;

  }
}
