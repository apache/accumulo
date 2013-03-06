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
package org.apache.accumulo.instamo;

import java.io.File;
import java.util.UUID;

import org.apache.accumulo.test.MiniAccumuloCluster;
import org.apache.accumulo.test.continuous.ContinuousIngest;
import org.apache.accumulo.test.continuous.ContinuousVerify;
import org.apache.commons.io.FileUtils;

/**
 * An example of running local map reduce against MiniAccumuloCluster
 */
public class MapReduceExample {

  public static void run(String instanceName, String zookeepers, String rootPassword, String args[]) throws Exception {

    // run continuous ingest to create data. This is not a map reduce job
    ContinuousIngest.main(new String[] {"-i", instanceName, "-z", zookeepers, "-u", "root", "-p", rootPassword, "--table", "ci", "--num", "5000000",
        "--batchMemory", "1000000"});

    String outputDir = FileUtils.getTempDirectoryPath() + File.separator + "ci_verify" + UUID.randomUUID().toString();

    try {
      // run verify map reduce job locally. This jobs looks for holes in the linked list create by continuous ingest
      ContinuousVerify.main(new String[] {"-D", "mapred.job.tracker=local", "-D", "fs.default.name=file:///", "-i", instanceName, "-z", zookeepers, "-u",
          "root", "-p", rootPassword, "--table", "ci", "--output", outputDir, "--maxMappers", "4", "--reducers", "1"});
    } finally {
      // delete output dir of mapreduce job
      FileUtils.deleteQuietly(new File(outputDir));
    }
  }

  public static void main(String[] args) throws Exception {
    File tmpDir = new File(FileUtils.getTempDirectory(), "macc-" + UUID.randomUUID().toString());

    try {
      MiniAccumuloCluster la = new MiniAccumuloCluster(tmpDir, "pass1234");
      la.start();

      System.out.println("\n   ---- Running Mapred Against Accumulo\n");

      run(la.getInstanceName(), la.getZooKeepers(), "pass1234", args);

      System.out.println("\n   ---- Ran Mapred Against Accumulo\n");

      la.stop();
    } finally {
      FileUtils.deleteQuietly(tmpDir);
    }
  }
}
