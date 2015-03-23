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
package org.apache.accumulo.core.client.mapreduce;

import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;

/**
 * Abstracts over configurations common to all InputSplits. Specifically it leaves out methods
 * related to number of ranges and locations per InputSplit as those vary by implementation.
 *
 * @see RangeInputSplit
 * @see MultiRangeInputSplit
 */
public interface AccumuloInputSplit {
  String getTableName();
  String getTableId();
  Instance getInstance();
  String getInstanceName();
  String getZooKeepers();
  String getPrincipal();
  AuthenticationToken getToken();
  Authorizations getAuths();
  Set<Pair<Text,Text>> getFetchedColumns();
  List<IteratorSetting> getIterators();
  Level getLogLevel();

  public float getProgress(Key currentKey);
}
