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
package org.apache.accumulo.core.util;

import java.net.InetSocketAddress;

import org.apache.hadoop.io.Text;
import org.apache.thrift.transport.TSocket;

public class AddressUtil {
  static public InetSocketAddress parseAddress(String address, int defaultPort) throws NumberFormatException {
    String[] parts = address.split(":", 2);
    if (address.contains("+"))
      parts = address.split("\\+", 2);
    if (parts.length == 2) {
      if (parts[1].isEmpty())
        return new InetSocketAddress(parts[0], defaultPort);
      return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    }
    return new InetSocketAddress(address, defaultPort);
  }
  
  static public InetSocketAddress parseAddress(Text address, int defaultPort) {
    return parseAddress(address.toString(), defaultPort);
  }
  
  static public TSocket createTSocket(String address, int defaultPort) {
    InetSocketAddress addr = parseAddress(address, defaultPort);
    return new TSocket(addr.getHostName(), addr.getPort());
  }
  
  static public String toString(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort();
  }
  
}
