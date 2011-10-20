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
package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class Verify extends Test {
  
  static byte[] zero = "0".getBytes();
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    ThreadPoolExecutor threadPool = Setup.getThreadPool(state);
    threadPool.shutdown();
    while (!threadPool.isTerminated()) {
      log.info("Waiting for " + (threadPool.getQueue().size() + threadPool.getActiveCount()) + " nodes to complete");
      threadPool.awaitTermination(10, TimeUnit.SECONDS);
    }
    
    String user = state.getConnector().whoami();
    Authorizations auths = state.getConnector().securityOperations().getUserAuthorizations(user);
    Scanner scanner = state.getConnector().createScanner(Setup.getTableName(), auths);
    for (Entry<Key,Value> entry : scanner) {
      byte[] value = entry.getValue().get();
      if (!Arrays.equals(value, zero)) throw new Exception("Bad key at " + entry);
    }
    log.info("Test successful on table " + Setup.getTableName());
  }
  
}
