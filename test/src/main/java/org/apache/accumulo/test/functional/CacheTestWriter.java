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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

public class CacheTestWriter {

  static final int NUM_DATA = 3;

  public static void main(String[] args) throws Exception {
    IZooReaderWriter zk = ZooReaderWriter.getInstance();

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

    Random r = new Random();

    while (count++ < numVerifications) {

      Map<String,String> expectedData = null;
      // change children in dir

      for (int u = 0; u < r.nextInt(4) + 1; u++) {
        expectedData = new TreeMap<>();

        if (r.nextFloat() < .5) {
          String child = UUID.randomUUID().toString();
          zk.putPersistentData(rootDir + "/dir/" + child, new byte[0], NodeExistsPolicy.SKIP);
          children.add(child);
        } else if (children.size() > 0) {
          int index = r.nextInt(children.size());
          String child = children.remove(index);
          zk.recursiveDelete(rootDir + "/dir/" + child, NodeMissingPolicy.FAIL);
        }

        for (String child : children) {
          expectedData.put(rootDir + "/dir/" + child, "");
        }

        // change values
        for (int i = 0; i < numData; i++) {
          byte data[] = Long.toString(r.nextLong(), 16).getBytes(UTF_8);
          zk.putPersistentData(rootDir + "/data" + i, data, NodeExistsPolicy.OVERWRITE);
          expectedData.put(rootDir + "/data" + i, new String(data, UTF_8));
        }

        // test a data node that does not always exists...
        if (r.nextFloat() < .5) {

          byte data[] = Long.toString(r.nextLong(), 16).getBytes(UTF_8);

          if (!dataSExists) {
            zk.putPersistentData(rootDir + "/dataS", data, NodeExistsPolicy.SKIP);
            dataSExists = true;
          } else {
            zk.putPersistentData(rootDir + "/dataS", data, NodeExistsPolicy.OVERWRITE);
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

          for (int i = 0; i < files.length; i++) {
            try {
              FileInputStream fis = new FileInputStream(files[i]);
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

          if (ok)
            break;
        }

        sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
      }
    }

    zk.putPersistentData(rootDir + "/die", new byte[0], NodeExistsPolicy.FAIL);
  }

}
