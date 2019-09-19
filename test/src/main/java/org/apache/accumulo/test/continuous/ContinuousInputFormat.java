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

package org.apache.accumulo.test.continuous;

import static org.apache.accumulo.test.continuous.ContinuousIngest.genCol;
import static org.apache.accumulo.test.continuous.ContinuousIngest.genLong;
import static org.apache.accumulo.test.continuous.ContinuousIngest.genRow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Generates a continuous ingest linked list per map reduce split. Each linked list is of
 * configurable length.
 */
public class ContinuousInputFormat extends InputFormat<Key,Value> {

  private static final String PROP_UUID = "mrbulk.uuid";
  private static final String PROP_MAP_TASK = "mrbulk.map.task";
  private static final String PROP_MAP_NODES = "mrbulk.map.nodes";
  private static final String PROP_ROW_MIN = "mrbulk.row.min";
  private static final String PROP_ROW_MAX = "mrbulk.row.max";
  private static final String PROP_FAM_MAX = "mrbulk.fam.max";
  private static final String PROP_QUAL_MAX = "mrbulk.qual.max";
  private static final String PROP_CHECKSUM = "mrbulk.checksum";
  private static final String PROP_VIS_FILE = "mrbulk.vis.file";

  private static class RandomSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput dataOutput) {}

    @Override
    public void readFields(DataInput dataInput) {}

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    int numTask = jobContext.getConfiguration().getInt(PROP_MAP_TASK, 1);
    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < numTask; i++) {
      splits.add(new RandomSplit());
    }
    return splits;
  }

  public static void configure(Configuration conf, String uuid, BulkIngest.Opts opts) {
    conf.set(PROP_UUID, uuid);
    conf.setInt(PROP_MAP_TASK, opts.mapTasks);
    conf.setLong(PROP_MAP_NODES, opts.mapNodes);
    conf.setLong(PROP_ROW_MIN, opts.min);
    conf.setLong(PROP_ROW_MAX, opts.max);
    conf.setInt(PROP_FAM_MAX, opts.maxColF);
    conf.setInt(PROP_QUAL_MAX, opts.maxColQ);
    if (opts.visFile != null)
      conf.set(PROP_VIS_FILE, opts.visFile);
    conf.setBoolean(PROP_CHECKSUM, opts.checksum);
  }

  @Override
  public RecordReader<Key,Value> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new RecordReader<Key,Value>() {
      long numNodes;
      long nodeCount;
      private Random random;

      private byte[] uuid;

      long minRow;
      long maxRow;
      int maxFam;
      int maxQual;
      List<ColumnVisibility> visibilities;
      boolean checksum;

      Key prevKey;
      Key currKey;
      Value currValue;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext job) {
        numNodes = job.getConfiguration().getLong(PROP_MAP_NODES, 1000000);
        uuid = job.getConfiguration().get(PROP_UUID).getBytes(StandardCharsets.UTF_8);

        minRow = job.getConfiguration().getLong(PROP_ROW_MIN, 0);
        maxRow = job.getConfiguration().getLong(PROP_ROW_MAX, Long.MAX_VALUE);
        maxFam = job.getConfiguration().getInt(PROP_FAM_MAX, Short.MAX_VALUE);
        maxQual = job.getConfiguration().getInt(PROP_QUAL_MAX, Short.MAX_VALUE);
        checksum = job.getConfiguration().getBoolean(PROP_CHECKSUM, false);
        String visFile = job.getConfiguration().get(PROP_VIS_FILE);
        if (visFile == null) {
          visibilities = Collections.singletonList(new ColumnVisibility());
        } else {
          visibilities = ContinuousIngest.readVisFromFile(visFile);
        }

        random = new Random(new SecureRandom().nextLong());

        nodeCount = 0;
      }

      private Key genKey(CRC32 cksum) {

        byte[] row = genRow(genLong(minRow, maxRow, random));

        byte[] fam = genCol(random.nextInt(maxFam));
        byte[] qual = genCol(random.nextInt(maxQual));
        byte[] cv = visibilities.get(random.nextInt(visibilities.size())).flatten();

        if (cksum != null) {
          cksum.update(row);
          cksum.update(fam);
          cksum.update(qual);
          cksum.update(cv);
        }

        return new Key(row, fam, qual, cv);
      }

      private byte[] createValue(byte[] ingestInstanceId, byte[] prevRow, Checksum cksum) {
        return ContinuousIngest.createValue(ingestInstanceId, nodeCount, prevRow, cksum);
      }

      @Override
      public boolean nextKeyValue() {

        if (nodeCount < numNodes) {
          CRC32 cksum = checksum ? new CRC32() : null;
          prevKey = currKey;
          byte[] prevRow = prevKey != null ? prevKey.getRowData().toArray() : null;
          currKey = genKey(cksum);
          currValue = new Value(createValue(uuid, prevRow, cksum));

          nodeCount++;
          return true;
        } else {
          return false;
        }
      }

      @Override
      public Key getCurrentKey() {
        return currKey;
      }

      @Override
      public Value getCurrentValue() {
        return currValue;
      }

      @Override
      public float getProgress() {
        return nodeCount * 1.0f / numNodes;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}
