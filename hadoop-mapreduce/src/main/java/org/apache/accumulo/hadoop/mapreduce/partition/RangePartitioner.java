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
package org.apache.accumulo.hadoop.mapreduce.partition;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Scanner;
import java.util.TreeSet;

import org.apache.accumulo.hadoopImpl.mapreduce.lib.DistributedCacheHelper;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Hadoop partitioner that uses ranges, and optionally sub-bins based on hashing.
 *
 * @since 2.0.0
 */
public class RangePartitioner extends Partitioner<Text,Writable> implements Configurable {
  private static final String PREFIX = RangePartitioner.class.getName();
  private static final String CUTFILE_KEY = PREFIX + ".cutFile";
  private static final String NUM_SUBBINS = PREFIX + ".subBins";

  private Configuration conf;

  @Override
  public int getPartition(Text key, Writable value, int numPartitions) {
    try {
      return findPartition(key, getCutPoints(), getNumSubBins());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  int findPartition(Text key, Text[] array, int numSubBins) {
    // find the bin for the range, and guarantee it is positive
    int index = Arrays.binarySearch(array, key);
    index = index < 0 ? (index + 1) * -1 : index;

    // both conditions work with numSubBins == 1, but this check is to avoid
    // hashing, when we don't need to, for speed
    if (numSubBins < 2) {
      return index;
    }
    return (key.toString().hashCode() & Integer.MAX_VALUE) % numSubBins + index * numSubBins;
  }

  private int _numSubBins = 0;

  private synchronized int getNumSubBins() {
    if (_numSubBins < 1) {
      // get number of sub-bins and guarantee it is positive
      _numSubBins = Math.max(1, getConf().getInt(NUM_SUBBINS, 1));
    }
    return _numSubBins;
  }

  private Text[] cutPointArray = null;

  private synchronized Text[] getCutPoints() throws IOException {
    if (cutPointArray == null) {
      String cutFileName = conf.get(CUTFILE_KEY);
      TreeSet<Text> cutPoints = new TreeSet<>();
      try (
          InputStream inputStream =
              DistributedCacheHelper.openCachedFile(cutFileName, CUTFILE_KEY, conf);
          Scanner in = new Scanner(inputStream, UTF_8)) {
        while (in.hasNextLine()) {
          cutPoints.add(new Text(Base64.getDecoder().decode(in.nextLine())));
        }
      }

      cutPointArray = cutPoints.toArray(new Text[cutPoints.size()]);

      if (cutPointArray == null) {
        throw new IOException("Cutpoint array not properly created from file" + cutFileName);
      }
    }
    return cutPointArray;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Sets the hdfs file name to use, containing a newline separated list of Base64 encoded split
   * points that represent ranges for partitioning
   */
  public static void setSplitFile(Job job, String file) {
    DistributedCacheHelper.addCacheFile(job, file, CUTFILE_KEY);
    job.getConfiguration().set(CUTFILE_KEY, file);
  }

  /**
   * Sets the number of random sub-bins per range
   */
  public static void setNumSubBins(Job job, int num) {
    job.getConfiguration().setInt(NUM_SUBBINS, num);
  }
}
