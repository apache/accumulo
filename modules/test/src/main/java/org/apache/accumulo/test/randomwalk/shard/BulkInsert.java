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
package org.apache.accumulo.test.randomwalk.shard;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class BulkInsert extends Test {

  class SeqfileBatchWriter implements BatchWriter {

    SequenceFile.Writer writer;

    SeqfileBatchWriter(Configuration conf, FileSystem fs, String file) throws IOException {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(fs.makeQualified(new Path(file))), SequenceFile.Writer.keyClass(Key.class),
          SequenceFile.Writer.valueClass(Value.class));
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      List<ColumnUpdate> updates = m.getUpdates();
      for (ColumnUpdate cu : updates) {
        Key key = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(), cu.getColumnVisibility(), Long.MAX_VALUE, false, false);
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
  public void visit(State state, Environment env, Properties props) throws Exception {

    String indexTableName = (String) state.get("indexTableName");
    String dataTableName = (String) state.get("docTableName");
    int numPartitions = (Integer) state.get("numPartitions");
    Random rand = (Random) state.get("rand");
    long nextDocID = (Long) state.get("nextDocID");

    int minInsert = Integer.parseInt(props.getProperty("minInsert"));
    int maxInsert = Integer.parseInt(props.getProperty("maxInsert"));
    int numToInsert = rand.nextInt(maxInsert - minInsert) + minInsert;

    int maxSplits = Integer.parseInt(props.getProperty("maxSplits"));

    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);

    String rootDir = "/tmp/shard_bulk/" + dataTableName;

    fs.mkdirs(new Path(rootDir));

    BatchWriter dataWriter = new SeqfileBatchWriter(conf, fs, rootDir + "/data.seq");
    BatchWriter indexWriter = new SeqfileBatchWriter(conf, fs, rootDir + "/index.seq");

    for (int i = 0; i < numToInsert; i++) {
      String docID = Insert.insertRandomDocument(nextDocID++, dataWriter, indexWriter, indexTableName, dataTableName, numPartitions, rand);
      log.debug("Bulk inserting document " + docID);
    }

    state.set("nextDocID", Long.valueOf(nextDocID));

    dataWriter.close();
    indexWriter.close();

    sort(state, env, fs, dataTableName, rootDir + "/data.seq", rootDir + "/data_bulk", rootDir + "/data_work", maxSplits);
    sort(state, env, fs, indexTableName, rootDir + "/index.seq", rootDir + "/index_bulk", rootDir + "/index_work", maxSplits);

    bulkImport(fs, state, env, dataTableName, rootDir, "data");
    bulkImport(fs, state, env, indexTableName, rootDir, "index");

    fs.delete(new Path(rootDir), true);
  }

  private void bulkImport(FileSystem fs, State state, Environment env, String tableName, String rootDir, String prefix) throws Exception {
    while (true) {
      String bulkDir = rootDir + "/" + prefix + "_bulk";
      String failDir = rootDir + "/" + prefix + "_failure";
      Path failPath = new Path(failDir);
      fs.delete(failPath, true);
      fs.mkdirs(failPath);
      env.getConnector().tableOperations().importDirectory(tableName, bulkDir, failDir, true);

      FileStatus[] failures = fs.listStatus(failPath);
      if (failures != null && failures.length > 0) {
        log.warn("Failed to bulk import some files, retrying ");

        for (FileStatus failure : failures) {
          if (!failure.getPath().getName().endsWith(".seq"))
            fs.rename(failure.getPath(), new Path(new Path(bulkDir), failure.getPath().getName()));
          else
            log.debug("Ignoring " + failure.getPath());
        }
        sleepUninterruptibly(3, TimeUnit.SECONDS);
      } else
        break;
    }
  }

  private void sort(State state, Environment env, FileSystem fs, String tableName, String seqFile, String outputDir, String workDir, int maxSplits)
      throws Exception {

    PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "/splits.txt"))), false, UTF_8.name());

    Connector conn = env.getConnector();

    Collection<Text> splits = conn.tableOperations().listSplits(tableName, maxSplits);
    for (Text split : splits)
      out.println(Base64.encodeBase64String(TextUtil.getBytes(split)));

    out.close();

    SortTool sortTool = new SortTool(seqFile, outputDir, workDir + "/splits.txt", splits);

    String[] args = new String[2];
    args[0] = "-libjars";
    args[1] = getMapReduceJars();

    if (ToolRunner.run(CachedConfiguration.getInstance(), sortTool, args) != 0) {
      throw new Exception("Failed to run map/red verify");
    }
  }

}
