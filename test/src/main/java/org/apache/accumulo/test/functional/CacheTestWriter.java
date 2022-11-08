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
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.harness.AccumuloITBase.random;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CacheTestWriter {

  static final int NUM_DATA = 3;

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "OBJECT_DESERIALIZATION"},
      justification = "path provided by test; object deserialization is okay for test")
  public static void main(String[] args) throws Exception {
    var zk = new ZooReaderWriter(SiteConfiguration.auto());

    String rootDir = args[0];
    File reportDir = new File(args[1]);
    int numReaders = Integer.parseInt(args[2]);
    int numVerifications = Integer.parseInt(args[3]);
    int numData = NUM_DATA;

    boolean dataSExists = false;
    int count = 0;

    zk.putPersistentData(rootDir, new byte[0], NodeExistsPolicy.FAIL);
    for (int i = 0; i < numData; i++) {
      zk.putPersistentData(rootDir + "/data" + i, new byte[0], NodeExistsPolicy.FAIL);
    }

    zk.putPersistentData(rootDir + "/dir", new byte[0], NodeExistsPolicy.FAIL);

    ArrayList<String> children = new ArrayList<>();

    while (count++ < numVerifications) {

      Map<String,String> expectedData = null;
      // change children in dir

      for (int u = 0; u < random.nextInt(4) + 1; u++) {
        expectedData = new TreeMap<>();

        if (random.nextFloat() < .5) {
          String child = UUID.randomUUID().toString();
          zk.putPersistentData(rootDir + "/dir/" + child, new byte[0], NodeExistsPolicy.SKIP);
          children.add(child);
        } else if (!children.isEmpty()) {
          int index = random.nextInt(children.size());
          String child = children.remove(index);
          zk.recursiveDelete(rootDir + "/dir/" + child, NodeMissingPolicy.FAIL);
        }

        for (String child : children) {
          expectedData.put(rootDir + "/dir/" + child, "");
        }

        // change values
        for (int i = 0; i < numData; i++) {
          byte[] data = Long.toString(random.nextLong(), 16).getBytes(UTF_8);
          zk.putPersistentData(rootDir + "/data" + i, data, NodeExistsPolicy.OVERWRITE);
          expectedData.put(rootDir + "/data" + i, new String(data, UTF_8));
        }

        // test a data node that does not always exists...
        if (random.nextFloat() < .5) {

          byte[] data = Long.toString(random.nextLong(), 16).getBytes(UTF_8);

          if (dataSExists) {
            zk.putPersistentData(rootDir + "/dataS", data, NodeExistsPolicy.OVERWRITE);
          } else {
            zk.putPersistentData(rootDir + "/dataS", data, NodeExistsPolicy.SKIP);
            dataSExists = true;
          }

          expectedData.put(rootDir + "/dataS", new String(data, UTF_8));

        } else {
          if (dataSExists) {
            zk.recursiveDelete(rootDir + "/dataS", NodeMissingPolicy.FAIL);
            dataSExists = false;
          }
        }
      }

      // change children in dir and change values

      System.out.println("expectedData " + expectedData);

      // wait for all readers to see changes
      while (true) {

        File[] files = reportDir.listFiles();
        if (files == null) {
          throw new IllegalStateException("report directory is inaccessible");
        }

        System.out.println("files.length " + files.length);

        if (files.length == numReaders) {
          boolean ok = true;

          for (File file : files) {
            try {
              FileInputStream fis = new FileInputStream(file);
              ObjectInputStream ois = new ObjectInputStream(fis);

              @SuppressWarnings("unchecked")
              Map<String,String> readerMap = (Map<String,String>) ois.readObject();

              fis.close();
              ois.close();

              System.out.println("read " + readerMap);

              if (!readerMap.equals(expectedData)) {
                System.out.println("maps not equals");
                ok = false;
              }
            } catch (IOException ioe) {
              // log.warn("Failed to read "+files[i], ioe);
              ok = false;
            }
          }

          if (ok) {
            break;
          }
        }

        sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
      }
    }

    zk.putPersistentData(rootDir + "/die", new byte[0], NodeExistsPolicy.FAIL);
  }

}
