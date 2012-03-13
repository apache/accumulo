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
package org.apache.accumulo.server.monitor.util.celltypes;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.util.AddressUtil;

public class LoggerLinkType extends CellType<RecoveryStatus> {
  
  @Override
  public String format(Object obj) {
    if (obj == null)
      return "-";
    RecoveryStatus status = (RecoveryStatus) obj;
    return String.format("<a href='/loggers?s=%s'>%s</a>", status.host, displayName(status));
  }
  
  public static String displayName(RecoveryStatus status) {
    if (status.host == null)
      return "--Unknown--";
    InetSocketAddress monitorAddress = AddressUtil.parseAddress(status.host, 0);
    return monitorAddress.getHostName() + ":" + monitorAddress.getPort();
  }
  
  @Override
  public int compare(RecoveryStatus o1, RecoveryStatus o2) {
    return displayName(o1).compareTo(displayName(o2));
  }
  
  @Override
  public String alignment() {
    return "left";
  }
  
}
