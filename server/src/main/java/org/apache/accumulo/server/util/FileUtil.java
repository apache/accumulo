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
package org.apache.accumulo.server.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

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
  
  private static final Logger log = Logger.getLogger(FileUtil.class);
  
  private static String createTmpDir(AccumuloConfiguration acuConf, VolumeManager fs) throws IOException {
    String accumuloDir = acuConf.get(Property.INSTANCE_DFS_DIR);
    
    String tmpDir = null;
    while (tmpDir == null) {
      tmpDir = accumuloDir + "/tmp/idxReduce_" + String.format("%09d", (int) (Math.random() * Integer.MAX_VALUE));
      
      try {
        fs.getFileStatus(new Path(tmpDir));
        tmpDir = null;
        continue;
      } catch (FileNotFoundException fne) {
        // found an unused temp directory
      }
      
      fs.mkdirs(new Path(tmpDir));
      
      // try to reserve the tmp dir
      if (!fs.createNewFile(new Path(tmpDir + "/__reserve")))
        tmpDir = null;
    }
    
    return tmpDir;
  }
  
  public static Collection<FileRef> reduceFiles(AccumuloConfiguration acuConf, Configuration conf, VolumeManager fs, Text prevEndRow, Text endRow,
      Collection<FileRef> mapFiles, int maxFiles, String tmpDir, int pass) throws IOException {
    ArrayList<FileRef> paths = new ArrayList<FileRef>(mapFiles);
    
    if (paths.size() <= maxFiles)
      return paths;
    
    String newDir = String.format("%s/pass_%04d", tmpDir, pass);
    
    int start = 0;
    
    ArrayList<FileRef> outFiles = new ArrayList<FileRef>();
    
    int count = 0;
    
    while (start < paths.size()) {
      int end = Math.min(maxFiles + start, paths.size());
      List<FileRef> inFiles = paths.subList(start, end);
      
      start = end;
      
      FileRef newMapFile = new FileRef(String.format("%s/%04d." + RFile.EXTENSION, newDir, count++));
      
      outFiles.add(newMapFile);
      FileSystem ns = fs.getFileSystemByPath(newMapFile.path());
      FileSKVWriter writer = new RFileOperations().openWriter(newMapFile.toString(), ns, ns.getConf(), acuConf);
      writer.startDefaultLocalityGroup();
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(inFiles.size());
      
      FileSKVIterator reader = null;
      try {
        for (FileRef s : inFiles) {
          reader = FileOperations.getInstance().openIndex(s.path().toString(), ns, ns.getConf(), acuConf);
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
          log.error(e, e);
        }
        
        for (SortedKeyValueIterator<Key,Value> r : iters)
          try {
            if (r != null)
              ((FileSKVIterator) r).close();
          } catch (IOException e) {
            // continue closing
            log.error(e, e);
          }
        
        try {
          if (writer != null)
            writer.close();
        } catch (IOException e) {
          log.error(e, e);
          throw e;
        }
      }
    }
    
    return reduceFiles(acuConf, conf, fs, prevEndRow, endRow, outFiles, maxFiles, tmpDir, pass + 1);
  }

  public static SortedMap<Double,Key> findMidPoint(VolumeManager fs, AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<FileRef> mapFiles,
      double minSplit) throws IOException {
    return findMidPoint(fs, acuConf, prevEndRow, endRow, mapFiles, minSplit, true);
  }
  
  public static double estimatePercentageLTE(VolumeManager fs, AccumuloConfiguration acuconf, Text prevEndRow, Text endRow, Collection<FileRef> mapFiles,
      Text splitRow) throws IOException {
    
    Configuration conf = CachedConfiguration.getInstance();
    
    String tmpDir = null;
    
    int maxToOpen = acuconf.getCount(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN);
    ArrayList<FileSKVIterator> readers = new ArrayList<FileSKVIterator>(mapFiles.size());
    
    try {
      if (mapFiles.size() > maxToOpen) {
        tmpDir = createTmpDir(acuconf, fs);
        
        log.debug("Too many indexes (" + mapFiles.size() + ") to open at once for " + endRow + " " + prevEndRow + ", reducing in tmpDir = " + tmpDir);
        
        long t1 = System.currentTimeMillis();
        mapFiles = reduceFiles(acuconf, conf, fs, prevEndRow, endRow, mapFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();
        
        log.debug("Finished reducing indexes for " + endRow + " " + prevEndRow + " in " + String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }
      
      if (prevEndRow == null)
        prevEndRow = new Text();
      
      long numKeys = 0;
      
      numKeys = countIndexEntries(acuconf, prevEndRow, endRow, mapFiles, true, conf, fs, readers);
      
      if (numKeys == 0) {
        // not enough info in the index to answer the question, so instead of going to
        // the data just punt and return .5
        return .5;
      }
      
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);
      
      // skip the prevendrow
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
        throw new RuntimeException("numLte > numKeys " + numLte + " " + numKeys + " " + prevEndRow + " " + endRow + " " + splitRow + " " + mapFiles);
      }
      
      // do not want to return 0% or 100%, so add 1 and 2 below
      return (numLte + 1) / (double) (numKeys + 2);
      
    } finally {
      cleanupIndexOp(acuconf, tmpDir, fs, readers);
    }
  }
  
  /**
   * 
   * @param mapFiles
   *          - list MapFiles to find the mid point key
   * 
   *          ISSUES : This method used the index files to find the mid point. If the map files have different index intervals this method will not return an
   *          accurate mid point. Also, it would be tricky to use this method in conjunction with an in memory map because the indexing interval is unknown.
   */
  public static SortedMap<Double,Key> findMidPoint(VolumeManager fs, AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<FileRef> mapFiles,
      double minSplit, boolean useIndex) throws IOException {
    Configuration conf = CachedConfiguration.getInstance();
    
    Collection<FileRef> origMapFiles = mapFiles;
    
    String tmpDir = null;
    
    int maxToOpen = acuConf.getCount(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN);
    ArrayList<FileSKVIterator> readers = new ArrayList<FileSKVIterator>(mapFiles.size());
    
    try {
      if (mapFiles.size() > maxToOpen) {
        if (!useIndex)
          throw new IOException("Cannot find mid point using data files, too many " + mapFiles.size());
        tmpDir = createTmpDir(acuConf, fs);
        
        log.debug("Too many indexes (" + mapFiles.size() + ") to open at once for " + endRow + " " + prevEndRow + ", reducing in tmpDir = " + tmpDir);
        
        long t1 = System.currentTimeMillis();
        mapFiles = reduceFiles(acuConf, conf, fs, prevEndRow, endRow, mapFiles, maxToOpen, tmpDir, 0);
        long t2 = System.currentTimeMillis();
        
        log.debug("Finished reducing indexes for " + endRow + " " + prevEndRow + " in " + String.format("%6.2f secs", (t2 - t1) / 1000.0));
      }
      
      if (prevEndRow == null)
        prevEndRow = new Text();
      
      long t1 = System.currentTimeMillis();
      
      long numKeys = 0;
      
      numKeys = countIndexEntries(acuConf, prevEndRow, endRow, mapFiles, tmpDir == null ? useIndex : false, conf, fs, readers);
      
      if (numKeys == 0) {
        if (useIndex) {
          log.warn("Failed to find mid point using indexes, falling back to data files which is slower. No entries between " + prevEndRow + " and " + endRow
              + " for " + mapFiles);
          // need to pass original map files, not possibly reduced indexes
          return findMidPoint(fs, acuConf, prevEndRow, endRow, origMapFiles, minSplit, false);
        }
        throw new IOException("Failed to find mid point, no entries between " + prevEndRow + " and " + endRow + " for " + mapFiles);
      }
      
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);
      
      // skip the prevendrow
      while (mmfi.hasTop() && mmfi.getTopKey().compareRow(prevEndRow) <= 0)
        mmfi.next();
      
      // read half of the keys in the index
      TreeMap<Double,Key> ret = new TreeMap<Double,Key>();
      Key lastKey = null;
      long keysRead = 0;
      
      Key keyBeforeMidPoint = null;
      long keyBeforeMidPointPosition = 0;
      
      while (keysRead < numKeys / 2) {
        if (lastKey != null && !lastKey.equals(mmfi.getTopKey(), PartialKey.ROW) && (keysRead - 1) / (double) numKeys >= minSplit) {
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
      
      log.debug(String.format("Found midPoint from indexes in %6.2f secs.%n", ((t2 - t1) / 1000.0)));
      
      ret.put(.5, mmfi.getTopKey());
      
      // sanity check
      for (Key key : ret.values()) {
        boolean inRange = (key.compareRow(prevEndRow) > 0 && (endRow == null || key.compareRow(endRow) < 1));
        if (!inRange) {
          throw new IOException("Found mid point is not in range " + key + " " + prevEndRow + " " + endRow + " " + mapFiles);
        }
      }
      
      return ret;
    } finally {
      cleanupIndexOp(acuConf, tmpDir, fs, readers);
    }
  }
  
  private static void cleanupIndexOp(AccumuloConfiguration acuConf, String tmpDir, VolumeManager fs, ArrayList<FileSKVIterator> readers) throws IOException {
    // close all of the index sequence files
    for (FileSKVIterator r : readers) {
      try {
        if (r != null)
          r.close();
      } catch (IOException e) {
        // okay, try to close the rest anyway
        log.error(e, e);
      }
    }
    
    if (tmpDir != null) {
      String tmpPrefix = acuConf.get(Property.INSTANCE_DFS_DIR) + "/tmp";
      if (tmpDir.startsWith(tmpPrefix))
        fs.deleteRecursively(new Path(tmpDir));
      else
        log.error("Did not delete tmp dir because it wasn't a tmp dir " + tmpDir);
    }
  }
  
  private static long countIndexEntries(AccumuloConfiguration acuConf, Text prevEndRow, Text endRow, Collection<FileRef> mapFiles, boolean useIndex,
      Configuration conf, VolumeManager fs, ArrayList<FileSKVIterator> readers) throws IOException {
    
    long numKeys = 0;
    
    // count the total number of index entries
    for (FileRef ref : mapFiles) {
      FileSKVIterator reader = null;
      Path path = ref.path();
      FileSystem ns = fs.getFileSystemByPath(path);
      try {
        if (useIndex)
          reader = FileOperations.getInstance().openIndex(path.toString(), ns, ns.getConf(), acuConf);
        else
          reader = FileOperations.getInstance().openReader(path.toString(), new Range(prevEndRow, false, null, true), LocalityGroupUtil.EMPTY_CF_SET, false, ns, ns.getConf(),
              acuConf);
        
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
          log.error(e, e);
        }
      }
      
      if (useIndex)
        readers.add(FileOperations.getInstance().openIndex(path.toString(), ns, ns.getConf(), acuConf));
      else
        readers.add(FileOperations.getInstance().openReader(path.toString(), new Range(prevEndRow, false, null, true), LocalityGroupUtil.EMPTY_CF_SET, false, ns, ns.getConf(),
            acuConf));
      
    }
    return numKeys;
  }
  
  public static Map<FileRef,FileInfo> tryToGetFirstAndLastRows(VolumeManager fs, AccumuloConfiguration acuConf, Set<FileRef> mapfiles) {
    
    HashMap<FileRef,FileInfo> mapFilesInfo = new HashMap<FileRef,FileInfo>();
    
    long t1 = System.currentTimeMillis();
    
    for (FileRef mapfile : mapfiles) {
      
      FileSKVIterator reader = null;
      FileSystem ns = fs.getFileSystemByPath(mapfile.path());
      try {
        reader = FileOperations.getInstance().openReader(mapfile.toString(), false, ns, ns.getConf(), acuConf);
        
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
    
    log.debug(String.format("Found first and last keys for %d map files in %6.2f secs", mapfiles.size(), (t2 - t1) / 1000.0));
    
    return mapFilesInfo;
  }
  
  public static WritableComparable<Key> findLastKey(VolumeManager fs, AccumuloConfiguration acuConf, Collection<FileRef> mapFiles) throws IOException {
    Key lastKey = null;
    
    for (FileRef ref : mapFiles) {
      Path path = ref.path();
      FileSystem ns = fs.getFileSystemByPath(path);
      FileSKVIterator reader = FileOperations.getInstance().openReader(path.toString(), true, ns, ns.getConf(), acuConf);
      
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
          log.error(e, e);
        }
      }
    }
    
    return lastKey;
    
  }
  
  private static class MLong {
    public MLong(long i) {
      l = i;
    }
    
    long l;
  }
  
  public static Map<KeyExtent,Long> estimateSizes(AccumuloConfiguration acuConf, Path mapFile, long fileSize, List<KeyExtent> extents, Configuration conf,
      VolumeManager fs) throws IOException {
    
    long totalIndexEntries = 0;
    Map<KeyExtent,MLong> counts = new TreeMap<KeyExtent,MLong>();
    for (KeyExtent keyExtent : extents)
      counts.put(keyExtent, new MLong(0));
    
    Text row = new Text();
    FileSystem ns = fs.getFileSystemByPath(mapFile);
    FileSKVIterator index = FileOperations.getInstance().openIndex(mapFile.toString(), ns, ns.getConf(), acuConf);
    
    try {
      while (index.hasTop()) {
        Key key = index.getTopKey();
        totalIndexEntries++;
        key.getRow(row);
        
        for (Entry<KeyExtent,MLong> entry : counts.entrySet())
          if (entry.getKey().contains(row))
            entry.getValue().l++;
        
        index.next();
      }
    } finally {
      try {
        if (index != null)
          index.close();
      } catch (IOException e) {
        // continue with next file
        log.error(e, e);
      }
    }
    
    Map<KeyExtent,Long> results = new TreeMap<KeyExtent,Long>();
    for (KeyExtent keyExtent : extents) {
      double numEntries = counts.get(keyExtent).l;
      if (numEntries == 0)
        numEntries = 1;
      long estSize = (long) ((numEntries / totalIndexEntries) * fileSize);
      results.put(keyExtent, estSize);
    }
    return results;
  }
  
}
