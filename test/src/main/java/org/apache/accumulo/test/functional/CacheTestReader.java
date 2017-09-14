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
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.slf4j.LoggerFactory;

public class CacheTestReader {
  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    String reportDir = args[1];
    String keepers = args[2];
    int numData = CacheTestWriter.NUM_DATA;

    File myfile = new File(reportDir + "/" + UUID.randomUUID());
    myfile.deleteOnExit();

    ZooCache zc = new ZooCache(keepers, 30000);

    while (true) {
      if (myfile.exists() && !myfile.delete()) {
        LoggerFactory.getLogger(CacheTestReader.class).warn("Unable to delete {}", myfile);
      }

      if (zc.get(rootDir + "/die") != null) {
        return;
      }

      Map<String,String> readData = new TreeMap<>();

      for (int i = 0; i < numData; i++) {
        byte[] v = zc.get(rootDir + "/data" + i);
        if (v != null)
          readData.put(rootDir + "/data" + i, new String(v, UTF_8));
      }

      byte[] v = zc.get(rootDir + "/dataS");
      if (v != null)
        readData.put(rootDir + "/dataS", new String(v, UTF_8));

      List<String> children = zc.getChildren(rootDir + "/dir");
      if (children != null)
        for (String child : children) {
          readData.put(rootDir + "/dir/" + child, "");
        }

      FileOutputStream fos = new FileOutputStream(myfile);
      ObjectOutputStream oos = new ObjectOutputStream(fos);

      oos.writeObject(readData);

      fos.close();
      oos.close();

      sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
    }

  }
}
