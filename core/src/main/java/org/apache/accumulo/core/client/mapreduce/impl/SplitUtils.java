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

package org.apache.accumulo.core.client.mapreduce.impl;

import java.math.BigInteger;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;

public class SplitUtils {

  /**
   * Central place to set common split configuration not handled by split constructors.
   * The intention is to make it harder to miss optional setters in future refactor.
   */
  public static void updateSplit(AccumuloInputSplit split,  Instance instance, InputTableConfig tableConfig,
                                  String principal, AuthenticationToken token, Authorizations auths, Level logLevel) {
    split.setInstanceName(instance.getInstanceName());
    split.setZooKeepers(instance.getZooKeepers());
    split.setMockInstance(instance instanceof MockInstance);

    split.setPrincipal(principal);
    split.setToken(token);
    split.setAuths(auths);

    split.setFetchedColumns(tableConfig.getFetchedColumns());
    split.setIterators(tableConfig.getIterators());
    split.setLogLevel(logLevel);
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
    BigInteger startBI = new BigInteger(AccumuloInputSplit.extractBytes(start, maxDepth));
    BigInteger endBI = new BigInteger(AccumuloInputSplit.extractBytes(end, maxDepth));
    BigInteger positionBI = new BigInteger(AccumuloInputSplit.extractBytes(position, maxDepth));
    return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
  }

}
