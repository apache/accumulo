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

import org.apache.accumulo.core.util.HostAndPort;

public class AddressUtil extends org.apache.accumulo.fate.util.AddressUtil {

  static public HostAndPort parseAddress(String address, boolean ignoreMissingPort) throws NumberFormatException {
    address = address.replace('+', ':');
    HostAndPort hap = HostAndPort.fromString(address);
    if (!ignoreMissingPort && !hap.hasPort())
      throw new IllegalArgumentException("Address was expected to contain port. address=" + address);

    return hap;
  }

  public static HostAndPort parseAddress(String address, int defaultPort) {
    return parseAddress(address, true).withDefaultPort(defaultPort);
  }
}
