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
package org.apache.accumulo.test.randomwalk.concurrent;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BulkImport extends Test {

  public static class RFileBatchWriter implements BatchWriter {

    RFile.Writer writer;

    public RFileBatchWriter(Configuration conf, FileSystem fs, String file) throws IOException {
      AccumuloConfiguration aconf = AccumuloConfiguration.getDefaultConfiguration();
      CachableBlockFile.Writer cbw = new CachableBlockFile.Writer(fs.create(new Path(file), false, conf.getInt("io.file.buffer.size", 4096),
          (short) conf.getInt("dfs.replication", 3), conf.getLong("dfs.block.size", 1 << 26)), "gz", conf, aconf);
      writer = new RFile.Writer(cbw, 100000);
      writer.startDefaultLocalityGroup();
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      List<ColumnUpdate> updates = m.getUpdates();
      for (ColumnUpdate cu : updates) {
        Key key = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(), cu.getColumnVisibility(), 42, false, false);
        Value val = new Value(cu.getValue(), false);

        try {
          writer.append(key, val);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
      for (Mutation mutation : iterable)
        addMutation(mutation);
    }

    @Override
    public void flush() throws MutationsRejectedException {}

    @Override
    public void close() throws MutationsRejectedException {
      try {
        writer.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    Random rand = (Random) state.get("rand");

    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");

    String tableName = tableNames.get(rand.nextInt(tableNames.size()));

    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);

    String bulkDir = "/tmp/concurrent_bulk/b_" + String.format("%016x", rand.nextLong() & 0x7fffffffffffffffl);

    fs.mkdirs(new Path(bulkDir));
    fs.mkdirs(new Path(bulkDir + "_f"));

    try {
      BatchWriter bw = new RFileBatchWriter(conf, fs, bulkDir + "/file01.rf");
      try {
        TreeSet<Long> rows = new TreeSet<Long>();
        int numRows = rand.nextInt(100000);
        for (int i = 0; i < numRows; i++) {
          rows.add(rand.nextLong() & 0x7fffffffffffffffl);
        }

        for (Long row : rows) {
          Mutation m = new Mutation(String.format("%016x", row));
          long val = rand.nextLong() & 0x7fffffffffffffffl;
          for (int j = 0; j < 10; j++) {
            m.put("cf", "cq" + j, new Value(String.format("%016x", val).getBytes(UTF_8)));
          }

          bw.addMutation(m);
        }
      } finally {
        bw.close();
      }

      conn.tableOperations().importDirectory(tableName, bulkDir, bulkDir + "_f", rand.nextBoolean());

      log.debug("BulkImported to " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("BulkImport " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException toe) {
      log.debug("BulkImport " + tableName + " failed, offline");
    } finally {
      fs.delete(new Path(bulkDir), true);
      fs.delete(new Path(bulkDir + "_f"), true);
    }

  }
}
