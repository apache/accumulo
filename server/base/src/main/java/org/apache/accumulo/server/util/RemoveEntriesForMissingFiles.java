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

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Remove file entries for map files that don't exist.
 */
public class RemoveEntriesForMissingFiles {

  static class Opts extends ServerUtilOpts {
    @Parameter(names = "--fix")
    boolean fix = false;
  }

  private static class CheckFileTask implements Runnable {
    private final Map<Path,Path> cache;
    private final VolumeManager fs;
    private final AtomicInteger missing;
    private final BatchWriter writer;
    private final Key key;
    private final Path path;
    private final Set<Path> processing;
    private final AtomicReference<Exception> exceptionRef;

    CheckFileTask(Map<Path,Path> cache, VolumeManager fs, AtomicInteger missing, BatchWriter writer,
        Key key, Path map, Set<Path> processing, AtomicReference<Exception> exceptionRef) {
      this.cache = cache;
      this.fs = fs;
      this.missing = missing;
      this.writer = writer;
      this.key = key;
      this.path = map;
      this.processing = processing;
      this.exceptionRef = exceptionRef;
    }

    @Override
    public void run() {
      try {
        if (fs.exists(path)) {
          synchronized (processing) {
            cache.put(path, path);
          }
        } else {
          missing.incrementAndGet();

          Mutation m = new Mutation(key.getRow());
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          if (writer != null) {
            writer.addMutation(m);
            System.out.println("Reference " + path + " removed from " + key.getRow());
          } else {
            System.out.println("File " + path + " is missing");
          }
        }
      } catch (Exception e) {
        exceptionRef.compareAndSet(null, e);
      } finally {
        synchronized (processing) {
          processing.remove(path);
          processing.notify();
        }
      }
    }
  }

  private static int checkTable(ServerContext context, String tableName, Range range, boolean fix)
      throws Exception {

    Map<Path,Path> cache = new LRUMap<>(100000);
    Set<Path> processing = new HashSet<>();
    ExecutorService threadPool = ThreadPools.getServerThreadPools().getPoolBuilder("CheckFileTasks")
        .numCoreThreads(16).build();

    System.out.printf("Scanning : %s %s\n", tableName, range);

    VolumeManager fs = context.getVolumeManager();
    Scanner metadata = context.createScanner(tableName, Authorizations.EMPTY);
    metadata.setRange(range);
    metadata.fetchColumnFamily(DataFileColumnFamily.NAME);
    int count = 0;
    AtomicInteger missing = new AtomicInteger(0);
    AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
    BatchWriter writer = null;

    if (fix) {
      writer = context.createBatchWriter(MetadataTable.NAME);
    }

    for (Entry<Key,Value> entry : metadata) {
      if (exceptionRef.get() != null) {
        break;
      }

      count++;
      Key key = entry.getKey();
      Path map = new Path(ValidationUtil.validate(key.getColumnQualifierData().toString()));

      synchronized (processing) {
        while (processing.size() >= 64 || processing.contains(map)) {
          processing.wait();
        }

        if (cache.get(map) != null) {
          continue;
        }

        processing.add(map);
      }

      threadPool.execute(
          new CheckFileTask(cache, fs, missing, writer, key, map, processing, exceptionRef));
    }

    threadPool.shutdown();

    synchronized (processing) {
      while (!processing.isEmpty()) {
        processing.wait();
      }
    }

    if (exceptionRef.get() != null) {
      throw new AccumuloException(exceptionRef.get());
    }

    if (writer != null && missing.get() > 0) {
      writer.close();
    }

    System.out.printf("Scan finished, %d files of %d missing\n\n", missing.get(), count);

    return missing.get();
  }

  static int checkAllTables(ServerContext context, boolean fix) throws Exception {
    int missing = checkTable(context, RootTable.NAME, TabletsSection.getRange(), fix);

    if (missing == 0) {
      return checkTable(context, MetadataTable.NAME, TabletsSection.getRange(), fix);
    } else {
      return missing;
    }
  }

  static int checkTable(ServerContext context, String tableName, boolean fix) throws Exception {
    if (tableName.equals(RootTable.NAME)) {
      throw new IllegalArgumentException("Can not check root table");
    } else if (tableName.equals(MetadataTable.NAME)) {
      return checkTable(context, RootTable.NAME, TabletsSection.getRange(), fix);
    } else {
      TableId tableId = context.getTableId(tableName);
      Range range = new KeyExtent(tableId, null, null).toMetaRange();
      return checkTable(context, MetadataTable.NAME, range, fix);
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(RemoveEntriesForMissingFiles.class.getName(), args);
    Span span = TraceUtil.startSpan(RemoveEntriesForMissingFiles.class, "main");
    try (Scope scope = span.makeCurrent()) {
      checkAllTables(opts.getServerContext(), opts.fix);
    } finally {
      span.end();
    }
  }
}
