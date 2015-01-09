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
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.map.MapFileOperations;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class DispatchingFileFactory extends FileOperations {

  private FileOperations findFileFactory(String file) {

    Path p = new Path(file);
    String name = p.getName();

    if (name.startsWith(Constants.MAPFILE_EXTENSION + "_")) {
      return new MapFileOperations();
    }
    String[] sp = name.split("\\.");

    if (sp.length < 2) {
      throw new IllegalArgumentException("File name " + name + " has no extension");
    }

    String extension = sp[sp.length - 1];

    if (extension.equals(Constants.MAPFILE_EXTENSION) || extension.equals(Constants.MAPFILE_EXTENSION + "_tmp")) {
      return new MapFileOperations();
    } else if (extension.equals(RFile.EXTENSION) || extension.equals(RFile.EXTENSION + "_tmp")) {
      return new RFileOperations();
    } else {
      throw new IllegalArgumentException("File type " + extension + " not supported");
    }
  }

  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return findFileFactory(file).openIndex(file, fs, conf, acuconf, null, null);
  }

  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    FileSKVIterator iter = findFileFactory(file).openReader(file, seekToBeginning, fs, conf, acuconf, null, null);
    if (acuconf.getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Reader(iter, acuconf);
    }
    return iter;
  }

  @Override
  public FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    FileSKVWriter writer = findFileFactory(file).openWriter(file, fs, conf, acuconf);
    if (acuconf.getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Writer(writer, acuconf);
    }
    return writer;
  }

  @Override
  public long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return findFileFactory(file).getFileSize(file, fs, conf, acuconf);
  }

  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf) throws IOException {
    return findFileFactory(file).openReader(file, range, columnFamilies, inclusive, fs, conf, tableConf, null, null);
  }

  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException {

    if (!tableConf.getBoolean(Property.TABLE_INDEXCACHE_ENABLED))
      indexCache = null;
    if (!tableConf.getBoolean(Property.TABLE_BLOCKCACHE_ENABLED))
      dataCache = null;

    return findFileFactory(file).openReader(file, range, columnFamilies, inclusive, fs, conf, tableConf, dataCache, indexCache);
  }

  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf,
      BlockCache dataCache, BlockCache indexCache) throws IOException {

    if (!acuconf.getBoolean(Property.TABLE_INDEXCACHE_ENABLED))
      indexCache = null;
    if (!acuconf.getBoolean(Property.TABLE_BLOCKCACHE_ENABLED))
      dataCache = null;

    FileSKVIterator iter = findFileFactory(file).openReader(file, seekToBeginning, fs, conf, acuconf, dataCache, indexCache);
    if (acuconf.getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Reader(iter, acuconf);
    }
    return iter;
  }

  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dCache, BlockCache iCache)
      throws IOException {

    if (!acuconf.getBoolean(Property.TABLE_INDEXCACHE_ENABLED))
      iCache = null;
    if (!acuconf.getBoolean(Property.TABLE_BLOCKCACHE_ENABLED))
      dCache = null;

    return findFileFactory(file).openIndex(file, fs, conf, acuconf, dCache, iCache);
  }

}

public abstract class FileOperations {

  private static final HashSet<String> validExtensions = new HashSet<String>(Arrays.asList(Constants.MAPFILE_EXTENSION, RFile.EXTENSION));

  public static Set<String> getValidExtensions() {
    return validExtensions;
  }

  public static String getNewFileExtension(AccumuloConfiguration acuconf) {
    return acuconf.get(Property.TABLE_FILE_TYPE);
  }

  public static FileOperations getInstance() {
    return new DispatchingFileFactory();
  }

  /**
   * Open a reader that will not be seeked giving an initial seek location. This is useful for file operations that only need to scan data within a range and do
   * not need to seek. Therefore file metadata such as indexes does not need to be kept in memory while the file is scanned. Also seek optimizations like bloom
   * filters do not need to be loaded.
   *
   */

  public abstract FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf) throws IOException;

  public abstract FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException;

  /**
   * Open a reader that fully support seeking and also enable any optimizations related to seeking, like bloom filters.
   *
   */

  public abstract FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf)
      throws IOException;

  public abstract FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf,
      BlockCache dataCache, BlockCache indexCache) throws IOException;

  public abstract FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;

  public abstract FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;

  public abstract FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dCache, BlockCache iCache)
      throws IOException;

  public abstract long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException;

}
