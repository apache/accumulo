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
package org.apache.accumulo.tserver.tablet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;

class ScanOptions {

  final Authorizations authorizations;
  final byte[] defaultLabels;
  final Set<Column> columnSet;
  final List<IterInfo> ssiList;
  final Map<String,Map<String,String>> ssio;
  final AtomicBoolean interruptFlag;
  final int num;
  final boolean isolated;

  ScanOptions(int num, Authorizations authorizations, byte[] defaultLabels, Set<Column> columnSet, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, AtomicBoolean interruptFlag, boolean isolated) {
    this.num = num;
    this.authorizations = authorizations;
    this.defaultLabels = defaultLabels;
    this.columnSet = columnSet;
    this.ssiList = ssiList;
    this.ssio = ssio;
    this.interruptFlag = interruptFlag;
    this.isolated = isolated;
  }

}