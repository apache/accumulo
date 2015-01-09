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
package org.apache.accumulo.start.classloader.vfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MiniDFSUtil {

  public static String computeDatanodeDirectoryPermission() {
    // MiniDFSCluster will check the permissions on the data directories, but does not
    // do a good job of setting them properly. We need to get the users umask and set
    // the appropriate Hadoop property so that the data directories will be created
    // with the correct permissions.
    try {
      Process p = Runtime.getRuntime().exec("/bin/sh -c umask");
      BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
      try {
        String line = bri.readLine();
        p.waitFor();

        Short umask = Short.parseShort(line.trim(), 8);
        // Need to set permission to 777 xor umask
        // leading zero makes java interpret as base 8
        int newPermission = 0777 ^ umask;

        return String.format("%03o", newPermission);
      } finally {
        bri.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error getting umask from O/S", e);
    }
  }

}
