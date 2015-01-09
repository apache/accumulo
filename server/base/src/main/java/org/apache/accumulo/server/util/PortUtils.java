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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class PortUtils {

  public static int getRandomFreePort() {
    Random r = new Random();
    int count = 0;

    while (count < 13) {
      int port = r.nextInt((1 << 16) - 1024) + 1024;

      ServerSocket so = null;
      try {
        so = new ServerSocket(port);
        so.setReuseAddress(true);
        return port;
      } catch (IOException ioe) {

      } finally {
        if (so != null)
          try {
            so.close();
          } catch (IOException e) {}
      }

    }

    throw new RuntimeException("Unable to find port");
  }

}
