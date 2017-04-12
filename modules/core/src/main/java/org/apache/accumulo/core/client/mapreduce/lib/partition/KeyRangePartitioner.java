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
package org.apache.accumulo.core.client.mapreduce.lib.partition;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Hadoop partitioner that uses ranges based on row keys, and optionally sub-bins based on hashing.
 */
public class KeyRangePartitioner extends Partitioner<Key,Writable> implements Configurable {
  private RangePartitioner rp = new RangePartitioner();

  @Override
  public int getPartition(Key key, Writable value, int numPartitions) {
    return rp.getPartition(key.getRow(), value, numPartitions);
  }

  @Override
  public Configuration getConf() {
    return rp.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    rp.setConf(conf);
  }

  /**
   * Sets the hdfs file name to use, containing a newline separated list of Base64 encoded split points that represent ranges for partitioning
   */
  public static void setSplitFile(Job job, String file) {
    RangePartitioner.setSplitFile(job, file);
  }

  /**
   * Sets the number of random sub-bins per range
   */
  public static void setNumSubBins(Job job, int num) {
    RangePartitioner.setNumSubBins(job, num);
  }
}
