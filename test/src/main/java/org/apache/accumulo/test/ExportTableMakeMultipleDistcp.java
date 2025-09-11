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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportTableMakeMultipleDistcp {
  private static final Logger log = LoggerFactory.getLogger(ExportTableMakeMultipleDistcp.class);

  @Test
  public void testDistcpFiles() {
    Set<String> volumeSet = new TreeSet<>();

    Map<String,String> uniqueFiles = new HashMap<>();
    uniqueFiles.put("F00000ja.rf",
        "hdfs://warehouse-b.com:6093/accumulo/tables/3/default_tablet/F00000ja.rf");
    uniqueFiles.put("F00000jb.rf",
        "hdfs://n1.example.com:6093/accumulo/tables/3/default_tablet/F00000jb.rf");
    uniqueFiles.put("C00000gf.rf",
        "hdfs://warehouse-a.com:6093/accumulo/tables/3/default_tablet/C00000gf.rf");
    uniqueFiles.put("F00000jc.rf",
        "hdfs://n3.example.com:6093/accumulo/tables/3/default_tablet/F00000jc.rf");
    uniqueFiles.put("F00000jd.rf",
        "hdfs://n2.example.com:6080/accumulo/tables/3/default_tablet/F00000jd.rf");
    uniqueFiles.put("F00000je.rf",
        "hdfs://warehouse-a.com:6093/accumulo/tables/3/default_tablet/F00000je.rf");
    uniqueFiles.put("F00000jz.rf",
        "hdfs://n1.example.com:6090/accumulo/tables/1/default_tablet/F00000jz.rf");
    uniqueFiles.put("F00000jf.rf",
        "hdfs://n2.example.com:6093/accumulo/tables/2/default_tablet/F00000jf.rf");
    uniqueFiles.put("A000002m.rf",
        "hdfs://n1.example.com:6093/accumulo/tables/1/default_tablet/A000002m.rf");
    uniqueFiles.put("Z00000ez.rf",
        "hdfs://n4.example.com:6093/accumulo/tables/3/default_tablet/Z00000ez.rf");
    uniqueFiles.put("A000002n.rf",
        "hdfs://warehouse-b.com:6093/accumulo/tables/1/default_tablet/A000002n.rf");
    uniqueFiles.put("A000002p.rf",
        "hdfs://n1.example.com:6093/accumulo/tables/3/default_tablet/A000002p.rf");
    uniqueFiles.put("A000002q.rf",
        "hdfs://n3.example.com:6093/accumulo/tables/3/default_tablet/A000002q.rf");
    uniqueFiles.put("C00000ef.rf",
        "hdfs://warehouse-a.com:6093/accumulo/tables/1/default_tablet/C00000ef.rf");
    uniqueFiles.put("F00000cz.rf",
        "hdfs://n1.example.com:6090/accumulo/tables/2/default_tablet/F00000cz.rf");
    uniqueFiles.put("C00000eg.rf",
        "hdfs://n1.example.com:6093/accumulo/tables/2/default_tablet/C00000eg.rf");
    uniqueFiles.put("C00000eh.rf",
        "hdfs://n3.example.com:6093/accumulo/tables/3/default_tablet/C00000eh.rf");
    uniqueFiles.put("D00000ef.rf",
        "hdfs://warehouse-a.com:6090/accumulo/tables/1/default_tablet/D00000ef.rf");
    uniqueFiles.put("C00000ek.rf",
        "hdfs://n2.example.com:6080/accumulo/tables/1/default_tablet/C00000ek.rf");
    uniqueFiles.put("C00000em.rf",
        "hdfs://n2.example.com:6093/accumulo/tables/2/default_tablet/C00000em.rf");
    uniqueFiles.put("S00000eo.rf",
        "hdfs://n4.example.com:6093/accumulo/tables/3/default_tablet/S00000eo.rf");
    uniqueFiles.put("Z00002fu.rf",
        "hdfs://n8.example.com:9999/accumulo/tables/1/default_tablet/Z00002fu.rf");

    // this will get each unique volume from original map and store it in a set
    for (String fileString : uniqueFiles.values()) {
      String[] fileSegmentArray = fileString.split("/");
      volumeSet.add(fileSegmentArray[2]);
    }

    // this will traverse original map, get all entries with same volume, add them to a new map, and
    // send new map to createDistcpFile method
    for (String volumeString : volumeSet) {
      Set<String> sortedVolumeSet = new TreeSet<>();
      for (String rFileString : uniqueFiles.values()) {
        if (rFileString.contains(volumeString)) {
          sortedVolumeSet.add(rFileString);
        }
      }
      createDistcpFile(volumeString, sortedVolumeSet);
    }
  }

  public static void createDistcpFile(String volumeName, Set<String> passedSet) {
    if (volumeName.contains(":")) {
      volumeName = volumeName.replace(":", "-");
    }

    try {
      String outputPath = System.getProperty("user.dir")
          + "/target/mini-tests/ExportTableMakeMultipleDistcp_createDistcpFile";
      Path outputFilePath = new Path(outputPath);
      FileSystem fs = FileSystem.getLocal(new Configuration());

      BufferedWriter distcpOut = new BufferedWriter(new OutputStreamWriter(
          fs.create(new Path(outputPath, "distcp-" + volumeName + ".txt")), UTF_8));

      try {
        for (String file : passedSet) {
          distcpOut.append(file);
          distcpOut.newLine();
        }

        distcpOut.close();
        distcpOut = null;

      } finally {
        if (distcpOut != null) {
          distcpOut.close();
        }
      }

      Path distcpPath = new Path(outputFilePath, "distcp-" + volumeName + ".txt");
      assertTrue(fs.exists(distcpPath), "Distcp file doesn't exist");

      // cleanup created files
      fs.deleteOnExit(distcpPath);
    } catch (Exception e) {
      System.out.println("Exception during configure");
    }
  }
}
