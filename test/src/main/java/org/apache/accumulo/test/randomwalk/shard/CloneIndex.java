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
package org.apache.accumulo.test.randomwalk.shard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class CloneIndex extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    String indexTableName = (String) state.get("indexTableName");
    String tmpIndexTableName = indexTableName + "_tmp";

    long t1 = System.currentTimeMillis();
    state.getConnector().tableOperations().flush(indexTableName, null, null, true);
    long t2 = System.currentTimeMillis();
    state.getConnector().tableOperations().clone(indexTableName, tmpIndexTableName, false, new HashMap<String,String>(), new HashSet<String>());
    long t3 = System.currentTimeMillis();

    log.debug("Cloned " + tmpIndexTableName + " from " + indexTableName + " flush: " + (t2 - t1) + "ms clone: " + (t3 - t2) + "ms");

  }

}
