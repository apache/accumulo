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
package org.apache.accumulo.core.file;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.map.MapFileOperations;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.summary.SummaryWriter;
import org.apache.hadoop.fs.Path;

class DispatchingFileFactory extends FileOperations {

  private FileOperations findFileFactory(FileOptions options) {
    String file = options.getFilename();

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

    if (extension.equals(Constants.MAPFILE_EXTENSION)
        || extension.equals(Constants.MAPFILE_EXTENSION + "_tmp")) {
      return new MapFileOperations();
    } else if (extension.equals(RFile.EXTENSION) || extension.equals(RFile.EXTENSION + "_tmp")) {
      return new RFileOperations();
    } else {
      throw new IllegalArgumentException("File type " + extension + " not supported");
    }
  }

  @Override
  protected long getFileSize(FileOptions options) throws IOException {
    return findFileFactory(options).getFileSize(options);
  }

  @Override
  protected FileSKVWriter openWriter(FileOptions options) throws IOException {
    FileOperations fileOps = new RFileOperations();
    FileSKVWriter writer = fileOps.openWriter(options);
    if (options.getTableConfiguration().getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      writer = new BloomFilterLayer.Writer(writer, options.getTableConfiguration(),
          options.isAccumuloStartEnabled());
    }

    return SummaryWriter.wrap(writer, options.getTableConfiguration(),
        options.isAccumuloStartEnabled());
  }

  @Override
  protected FileSKVIterator openIndex(FileOptions options) throws IOException {
    return findFileFactory(options).openIndex(options);
  }

  @Override
  protected FileSKVIterator openReader(FileOptions options) throws IOException {
    FileSKVIterator iter = findFileFactory(options).openReader(options);
    if (options.getTableConfiguration().getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Reader(iter, options.getTableConfiguration());
    } else {
      return iter;
    }
  }

  @Override
  protected FileSKVIterator openScanReader(FileOptions options) throws IOException {
    return findFileFactory(options).openScanReader(options);
  }
}
