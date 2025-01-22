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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CacheTestReader {

  private static final Logger LOG = LoggerFactory.getLogger(CacheTestReader.class);

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    String reportDir = args[1];
    String keepers = args[2];
    int numData = CacheTestWriter.NUM_DATA;

    File myfile = new File(reportDir + "/" + UUID.randomUUID());
    myfile.deleteOnExit();

    try {
      try (var zk = new ZooSession(CacheTestReader.class.getSimpleName(), keepers, 30_000, null)) {
        ZooCache zc = zk.getCache(Set.of("/")).get();

        while (true) {
          if (myfile.exists() && !myfile.delete()) {
            LOG.warn("Unable to delete {}", myfile);
          }

          if (zc.get(rootDir + "/die") != null) {
            LOG.info("Exiting");
            return;
          }

          Map<String,String> readData = new TreeMap<>();

          for (int i = 0; i < numData; i++) {
            byte[] v = zc.get(rootDir + "/data" + i);
            if (v != null) {
              readData.put(rootDir + "/data" + i, new String(v, UTF_8));
            }
          }

          byte[] v = zc.get(rootDir + "/dataS");
          if (v != null) {
            readData.put(rootDir + "/dataS", new String(v, UTF_8));
          }

          List<String> children = zc.getChildren(rootDir + "/dir");
          if (children != null) {
            for (String child : children) {
              readData.put(rootDir + "/dir/" + child, "");
            }
          }

          LOG.info("Data: {}", readData);

          try (FileOutputStream fos = new FileOutputStream(myfile);
              ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(readData);
          }

          Thread.sleep(20);
        }
      }
    } catch (Exception e) {
      LOG.error("CacheTestReader thread failed", e);
    }

  }
}
