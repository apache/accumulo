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

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Remove file entries for map files that don't exist.
 *
 */
public class RemoveEntriesForMissingFiles {

  static class Opts extends ClientOpts {
    @Parameter(names = "--fix")
    boolean fix = false;
  }

  private static class CheckFileTask implements Runnable {
    @SuppressWarnings("rawtypes")
    private Map cache;
    private VolumeManager fs;
    private AtomicInteger missing;
    private BatchWriter writer;
    private Key key;
    private Path path;
    private Set<Path> processing;
    private AtomicReference<Exception> exceptionRef;

    @SuppressWarnings({"rawtypes"})
    CheckFileTask(Map cache, VolumeManager fs, AtomicInteger missing, BatchWriter writer, Key key, Path map, Set<Path> processing,
        AtomicReference<Exception> exceptionRef) {
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
    @SuppressWarnings("unchecked")
    public void run() {
      try {
        if (!fs.exists(path)) {
          missing.incrementAndGet();

          Mutation m = new Mutation(key.getRow());
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          if (writer != null) {
            writer.addMutation(m);
            System.out.println("Reference " + path + " removed from " + key.getRow());
          } else {
            System.out.println("File " + path + " is missing");
          }
        } else {
          synchronized (processing) {
            cache.put(path, path);
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

  private static int checkTable(Instance instance, String principal, AuthenticationToken token, String table, Range range, boolean fix) throws Exception {

    @SuppressWarnings({"rawtypes"})
    Map cache = new LRUMap(100000);
    Set<Path> processing = new HashSet<Path>();
    ExecutorService threadPool = Executors.newFixedThreadPool(16);

    System.out.printf("Scanning : %s %s\n", table, range);

    VolumeManager fs = VolumeManagerImpl.get();
    Connector connector = instance.getConnector(principal, token);
    Scanner metadata = connector.createScanner(table, Authorizations.EMPTY);
    metadata.setRange(range);
    metadata.fetchColumnFamily(DataFileColumnFamily.NAME);
    int count = 0;
    AtomicInteger missing = new AtomicInteger(0);
    AtomicReference<Exception> exceptionRef = new AtomicReference<Exception>(null);
    BatchWriter writer = null;

    if (fix)
      writer = connector.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

    for (Entry<Key,Value> entry : metadata) {
      if (exceptionRef.get() != null)
        break;

      count++;
      Key key = entry.getKey();
      Path map = fs.getFullPath(key);

      synchronized (processing) {
        while (processing.size() >= 64 || processing.contains(map))
          processing.wait();

        if (cache.get(map) != null) {
          continue;
        }

        processing.add(map);
      }

      threadPool.submit(new CheckFileTask(cache, fs, missing, writer, key, map, processing, exceptionRef));
    }

    threadPool.shutdown();

    synchronized (processing) {
      while (processing.size() > 0)
        processing.wait();
    }

    if (exceptionRef.get() != null)
      throw new AccumuloException(exceptionRef.get());

    if (writer != null && missing.get() > 0)
      writer.close();

    System.out.printf("Scan finished, %d files of %d missing\n\n", missing.get(), count);

    return missing.get();
  }

  static int checkAllTables(Instance instance, String principal, AuthenticationToken token, boolean fix) throws Exception {
    int missing = checkTable(instance, principal, token, RootTable.NAME, MetadataSchema.TabletsSection.getRange(), fix);

    if (missing == 0)
      return checkTable(instance, principal, token, MetadataTable.NAME, MetadataSchema.TabletsSection.getRange(), fix);
    else
      return missing;
  }

  static int checkTable(Instance instance, String principal, AuthenticationToken token, String tableName, boolean fix) throws Exception {
    if (tableName.equals(RootTable.NAME)) {
      throw new IllegalArgumentException("Can not check root table");
    } else if (tableName.equals(MetadataTable.NAME)) {
      return checkTable(instance, principal, token, RootTable.NAME, MetadataSchema.TabletsSection.getRange(), fix);
    } else {
      String tableId = Tables.getTableId(instance, tableName);
      Range range = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
      return checkTable(instance, principal, token, MetadataTable.NAME, range, fix);
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(RemoveEntriesForMissingFiles.class.getName(), args, scanOpts, bwOpts);

    checkAllTables(opts.getInstance(), opts.principal, opts.getToken(), opts.fix);
  }
}
