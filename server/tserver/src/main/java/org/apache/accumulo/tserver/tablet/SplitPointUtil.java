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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitPointUtil {
  private static final Logger log = LoggerFactory.getLogger(SplitPointUtil.class);
  private static final int DEFAULT_BATCH_SIZE = 200;

  public static Optional<Key> findSplitPoint(List<StoredTabletFile> files, int maxOpen,
      FileSystem fs, Configuration hadoopConf, TableConfiguration tableConf) throws IOException {

    if (files.size() <= maxOpen) {
      return computeMidpoint(files, fs, hadoopConf, tableConf);
    }

    log.info("Tablet has {} files, reducing using batch size {}", files.size(), DEFAULT_BATCH_SIZE);
    List<StoredTabletFile> reduced =
        reduceFiles(files, DEFAULT_BATCH_SIZE, fs, hadoopConf, tableConf);

    return computeMidpoint(reduced, fs, hadoopConf, tableConf);
  }

  private static Optional<Key> computeMidpoint(List<StoredTabletFile> files, FileSystem fs,
      Configuration conf, TableConfiguration tableConf) throws IOException {
    List<Key> allKeys = new ArrayList<>();

    for (StoredTabletFile file : files) {
      try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(file, fs, conf, tableConf.getCryptoService()).withTableConfiguration(tableConf)
          .build()) {

        reader.seek(new Range(), Collections.emptyList(), false);

        while (reader.hasTop()) {
          allKeys.add(new Key(reader.getTopKey()));
          reader.next();
        }
      }
    }

    if (allKeys.isEmpty()) {
      return Optional.empty();
    }

    Collections.sort(allKeys);
    return Optional.of(allKeys.get(allKeys.size() / 2));
  }

  private static List<StoredTabletFile> reduceFiles(List<StoredTabletFile> files, int batchSize,
      FileSystem fs, Configuration conf, TableConfiguration tableConf) throws IOException {
    List<StoredTabletFile> reduced = new ArrayList<>();

    for (int i = 0; i < files.size(); i += batchSize) {
      List<StoredTabletFile> batch = files.subList(i, Math.min(i + batchSize, files.size()));
      StoredTabletFile merged = mergeBatch(batch, fs, conf, tableConf);
      reduced.add(merged);
    }

    if (reduced.size() > batchSize) {
      return reduceFiles(reduced, batchSize, fs, conf, tableConf);
    }

    return reduced;
  }

  private static StoredTabletFile mergeBatch(List<StoredTabletFile> batch, FileSystem fs,
      Configuration conf, TableConfiguration tableConf) throws IOException {
    Path tmpDir = new Path("/tmp/idxReduce_" + System.nanoTime());
    fs.mkdirs(tmpDir);
    Path mergedPath = new Path(tmpDir, "merged_" + UUID.randomUUID() + ".rf");

    Map<String,String> props = new HashMap<>();
    tableConf.iterator().forEachRemaining(e -> props.put(e.getKey(), e.getValue()));

    try (RFileWriter writer = RFile.newWriter().to(mergedPath.toString()).withFileSystem(fs)
        .withTableProperties(props).build()) {

      for (StoredTabletFile file : batch) {
        try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(file, fs, conf, tableConf.getCryptoService()).withTableConfiguration(tableConf)
            .build()) {

          reader.seek(new Range(), Collections.emptyList(), false);
          while (reader.hasTop()) {
            writer.append(reader.getTopKey(), reader.getTopValue());
            reader.next();
          }
        }
      }

    }

    return new StoredTabletFile(mergedPath.toString());
  }
}
