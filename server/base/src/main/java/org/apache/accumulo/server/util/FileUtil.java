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

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
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
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {

  public static class FileInfo {
    final Text firstRow;
    final Text lastRow;

    public FileInfo(Text firstRow, Text lastRow) {
      this.firstRow = firstRow;
      this.lastRow = lastRow;
    }

    public Text getFirstRow() {
      return firstRow;
    }

    public Text getLastRow() {
      return lastRow;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

  private static Path createTmpDir(ServerContext context, String tabletDirectory)
      throws IOException {

    VolumeManager fs = context.getVolumeManager();

    Path result = null;
    while (result == null) {
      result = new Path(tabletDirectory + Path.SEPARATOR + "tmp/idxReduce_"
          + String.format("%09d", RANDOM.get().nextInt(Integer.MAX_VALUE)));
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
      if (!fs.createNewFile(new Path(result, "__reserve"))) {
        result = null;
      }
    }
    return result;
  }

  public static Collection<? extends TabletFile> reduceFiles(ServerContext context,
      TableConfiguration tableConf, Text prevEndRow, Text endRow,
      Collection<? extends TabletFile> dataFiles, int maxFiles, Path tmpDir, int pass)
      throws IOException {

    ArrayList<? extends TabletFile> files = new ArrayList<>(dataFiles);

    if (files.size() <= maxFiles) {
      return files;
    }

    String newDir = String.format("%s/pass_%04d", tmpDir, pass);

    int start = 0;

    ArrayList<UnreferencedTabletFile> outFiles = new ArrayList<>();

    int count = 0;

    while (start < files.size()) {
      int end = Math.min(maxFiles + start, files.size());
      List<? extends TabletFile> inFiles = files.subList(start, end);

      start = end;

      // temporary tablet file does not conform to typical path verified in TabletFile
      Path newPath = new Path(String.format("%s/%04d.%s", newDir, count++, RFile.EXTENSION));
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(newPath);
      UnreferencedTabletFile newDataFile = UnreferencedTabletFile.of(ns, newPath);

      outFiles.add(newDataFile);
      FileSKVWriter writer = new RFileOperations().newWriterBuilder()
          .forFile(newDataFile, ns, ns.getConf(), tableConf.getCryptoService())
          .withTableConfiguration(tableConf).build();
      writer.startDefaultLocalityGroup();
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(inFiles.size());

      FileSKVIterator reader = null;
      try {
        for (TabletFile file : inFiles) {
          ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
          reader = FileOperations.getInstance().newIndexReaderBuilder()
              .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
              .withTableConfiguration(tableConf).build();
          iters.add(reader);
        }

        MultiIterator mmfi = new MultiIterator(iters, true);

        while (mmfi.hasTop()) {
          Key key = mmfi.getTopKey();

          boolean gtPrevEndRow = prevEndRow == null || key.compareRow(prevEndRow) > 0;
          boolean lteEndRow = endRow == null || key.compareRow(endRow) <= 0;

          if (gtPrevEndRow && lteEndRow) {
            writer.append(key, new Value());
          }

          if (!lteEndRow) {
            break;
          }

          mmfi.next();
        }
      } finally {
        try {
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }

        for (SortedKeyValueIterator<Key,Value> r : iters) {
          try {
            if (r != null) {
              ((FileSKVIterator) r).close();
            }
          } catch (IOException e) {
            // continue closing
            log.error("{}", e.getMessage(), e);
          }
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
      String tabletDir, Text prevEndRow, Text endRow, Collection<? extends TabletFile> dataFiles,
      Text splitRow) throws IOException {

    Path tmpDir = null;

    int maxToOpen = getMaxFilesToOpen(context.getConfiguration());
    ArrayList<FileSKVIterator> readers = new ArrayList<>(dataFiles.size());

    try {
      if (dataFiles.size() > maxToOpen) {
        tmpDir = createTmpDir(context, tabletDir);

        log.debug("Too many indexes ({}) to open at once for {} {}, reducing in tmpDir = {}",
            dataFiles.size(), endRow, prevEndRow, tmpDir);

        long t1 = System.currentTimeMillis();
        dataFiles =
            reduceFiles(context, tableConf, prevEndRow, endRow, dataFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();

        log.debug("Finished reducing indexes for {} {} in {}", endRow, prevEndRow,
            String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }

      if (prevEndRow == null) {
        prevEndRow = new Text();
      }

      long numKeys =
          countIndexEntries(context, tableConf, prevEndRow, endRow, dataFiles, true, readers);

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
        throw new IllegalStateException("numLte > numKeys " + numLte + " " + numKeys + " "
            + prevEndRow + " " + endRow + " " + splitRow + " " + dataFiles);
      }

      // do not want to return 0% or 100%, so add 1 and 2 below
      return (numLte + 1) / (double) (numKeys + 2);

    } finally {
      cleanupIndexOp(tmpDir, context.getVolumeManager(), readers);
    }
  }

  @SuppressWarnings("deprecation")
  private static int getMaxFilesToOpen(AccumuloConfiguration conf) {
    return conf.getCount(
        conf.resolve(Property.SPLIT_MAXOPEN, Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN));
  }

  /**
   *
   * @param dataFiles - list of data files to find the mid point key
   *
   *        ISSUES : This method used the index files to find the mid point. If the data files have
   *        different index intervals this method will not return an accurate mid point. Also, it
   *        would be tricky to use this method in conjunction with an in memory map because the
   *        indexing interval is unknown.
   */
  public static SortedMap<Double,Key> findMidPoint(ServerContext context,
      TableConfiguration tableConf, String tabletDirectory, Text prevEndRow, Text endRow,
      Collection<? extends TabletFile> dataFiles, double minSplit, boolean useIndex)
      throws IOException {

    Collection<? extends TabletFile> origDataFiles = dataFiles;

    Path tmpDir = null;

    int maxToOpen = getMaxFilesToOpen(context.getConfiguration());
    ArrayList<FileSKVIterator> readers = new ArrayList<>(dataFiles.size());

    try {
      if (dataFiles.size() > maxToOpen) {
        if (!useIndex) {
          throw new IOException(
              "Cannot find mid point using data files, too many " + dataFiles.size());
        }
        tmpDir = createTmpDir(context, tabletDirectory);

        log.debug("Too many indexes ({}) to open at once for {} {}, reducing in tmpDir = {}",
            dataFiles.size(), endRow, prevEndRow, tmpDir);

        long t1 = System.currentTimeMillis();
        dataFiles =
            reduceFiles(context, tableConf, prevEndRow, endRow, dataFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();

        log.debug("Finished reducing indexes for {} {} in {}", endRow, prevEndRow,
            String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }

      if (prevEndRow == null) {
        prevEndRow = new Text();
      }

      long t1 = System.currentTimeMillis();

      long numKeys = countIndexEntries(context, tableConf, prevEndRow, endRow, dataFiles,
          tmpDir == null ? useIndex : false, readers);

      if (numKeys == 0) {
        if (useIndex) {
          log.warn(
              "Failed to find mid point using indexes, falling back to"
                  + " data files which is slower. No entries between {} and {} for {}",
              prevEndRow, endRow, dataFiles);
          // need to pass original data files, not possibly reduced indexes
          return findMidPoint(context, tableConf, tabletDirectory, prevEndRow, endRow,
              origDataFiles, minSplit, false);
        }
        return Collections.emptySortedMap();
      }

      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);

      // skip the prevEndRow
      while (mmfi.hasTop() && mmfi.getTopKey().compareRow(prevEndRow) <= 0) {
        mmfi.next();
      }

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

        if (lastKey == null) {
          lastKey = new Key();
        }

        lastKey.set(mmfi.getTopKey());

        keysRead++;

        // consume minimum
        mmfi.next();
      }

      if (keyBeforeMidPoint != null) {
        ret.put(keyBeforeMidPointPosition / (double) numKeys, keyBeforeMidPoint);
      }

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
              + endRow + " " + dataFiles);
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
        if (r != null) {
          r.close();
        }
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
      Text prevEndRow, Text endRow, Collection<? extends TabletFile> dataFiles, boolean useIndex,
      ArrayList<FileSKVIterator> readers) throws IOException {
    long numKeys = 0;

    // count the total number of index entries
    for (TabletFile file : dataFiles) {
      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
      try {
        if (useIndex) {
          reader = FileOperations.getInstance().newIndexReaderBuilder()
              .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
              .withTableConfiguration(tableConf).build();
        } else {
          reader = FileOperations.getInstance().newScanReaderBuilder()
              .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
              .withTableConfiguration(tableConf)
              .overRange(new Range(prevEndRow, false, null, true), Set.of(), false).build();
        }

        while (reader.hasTop()) {
          Key key = reader.getTopKey();
          if (endRow != null && key.compareRow(endRow) > 0) {
            break;
          } else if (prevEndRow == null || key.compareRow(prevEndRow) > 0) {
            numKeys++;
          }

          reader.next();
        }
      } finally {
        try {
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }
      }

      if (useIndex) {
        readers.add(FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).build());
      } else {
        readers.add(FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf)
            .overRange(new Range(prevEndRow, false, null, true), Set.of(), false).build());
      }

    }
    return numKeys;
  }

  public static <T extends TabletFile> Map<T,FileInfo> tryToGetFirstAndLastRows(
      ServerContext context, TableConfiguration tableConf, Set<T> dataFiles) {

    HashMap<T,FileInfo> dataFilesInfo = new HashMap<>();

    long t1 = System.currentTimeMillis();

    for (T dataFile : dataFiles) {

      FileSKVIterator reader = null;
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(dataFile.getPath());
      try {
        reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(dataFile, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).build();

        Text firstRow = reader.getFirstRow();
        if (firstRow != null) {
          dataFilesInfo.put(dataFile, new FileInfo(firstRow, reader.getLastRow()));
        }

      } catch (IOException ioe) {
        log.warn("Failed to read data file to determine first and last key : " + dataFile, ioe);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ioe) {
            log.warn("failed to close " + dataFile, ioe);
          }
        }
      }

    }

    long t2 = System.currentTimeMillis();

    log.debug(String.format("Found first and last keys for %d data files in %6.2f secs",
        dataFiles.size(), (t2 - t1) / 1000.0));

    return dataFilesInfo;
  }

  public static <T extends TabletFile> Text findLastRow(ServerContext context,
      TableConfiguration tableConf, Collection<T> dataFiles) throws IOException {

    Text lastRow = null;

    for (T file : dataFiles) {
      FileSystem ns = context.getVolumeManager().getFileSystemByPath(file.getPath());
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
          .withTableConfiguration(tableConf).seekToBeginning().build();

      try {
        if (!reader.hasTop()) {
          // file is empty, so there is no last key
          continue;
        }

        Text row = reader.getLastRow();

        if (lastRow == null || row.compareTo(lastRow) > 0) {
          lastRow = row;
        }
      } finally {
        try {
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          log.error("{}", e.getMessage(), e);
        }
      }
    }

    return lastRow;

  }

  /**
   * Convert TabletFiles to Strings in case we need to reduce number of files. The temporary files
   * used will have irregular paths that don't conform to TabletFile verification.
   */
  public static Collection<String> toPathStrings(Collection<ReferencedTabletFile> files) {
    return files.stream().map(ReferencedTabletFile::getNormalizedPathStr)
        .collect(Collectors.toList());
  }
}
