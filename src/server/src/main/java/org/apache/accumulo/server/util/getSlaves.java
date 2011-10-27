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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.monitor.servlets.BasicServlet;

/**
 * Function to get the list of expected tablet servers.
 * 
 * @author Accumulo Team
 * 
 */
public class getSlaves {
  
  public static final List<String> main() throws IOException {
    List<String> result = new ArrayList<String>();
    InputStream input = BasicServlet.class.getClassLoader().getResourceAsStream("slaves");
    byte[] buffer = new byte[1024];
    int n;
    StringBuilder all = new StringBuilder();
    while ((n = input.read(buffer)) > 0)
      all.append(new String(buffer, 0, n));
    for (String slave : all.toString().split("\n")) {
      slave = slave.trim();
      if (slave.length() > 0 && slave.indexOf("#") < 0)
        result.add(slave);
    }
    return result;
  }
}