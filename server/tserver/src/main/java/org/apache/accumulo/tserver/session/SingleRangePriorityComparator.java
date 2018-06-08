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
package org.apache.accumulo.tserver.session;

import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.spi.scan.ScanInfo.Type;

//TODO no docs
//TODO location
public class SingleRangePriorityComparator extends DefaultSessionComparator {

  @Override
  public int compare(ScanInfo si1, ScanInfo si2) {
    int priority = super.compare(si1, si2);

    if (si1.getScanType() == Type.MULTI && si2.getScanType() == Type.SINGLE) {
      if (priority < 0) {
        priority *= -1;
      }
    } else if (si2.getScanType() == Type.MULTI && si1.getScanType() == Type.SINGLE) {
      if (priority > 0) {
        priority *= -1;
      }
    }

    return priority;
  }
}
