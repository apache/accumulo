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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class VerifyIndex extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    String indexTableName = (String) state.get("indexTableName");
    String tmpIndexTableName = indexTableName + "_tmp";

    // scan new and old index and verify identical
    Scanner indexScanner1 = state.getConnector().createScanner(tmpIndexTableName, Authorizations.EMPTY);
    Scanner indexScanner2 = state.getConnector().createScanner(indexTableName, Authorizations.EMPTY);

    Iterator<Entry<Key,Value>> iter = indexScanner2.iterator();

    int count = 0;

    for (Entry<Key,Value> entry : indexScanner1) {
      if (!iter.hasNext())
        throw new Exception("index rebuild mismatch " + entry.getKey() + " " + indexTableName);

      Key key1 = entry.getKey();
      Key key2 = iter.next().getKey();

      if (!key1.equals(key2, PartialKey.ROW_COLFAM_COLQUAL))
        throw new Exception("index rebuild mismatch " + key1 + " " + key2 + " " + indexTableName + " " + tmpIndexTableName);
      count++;
      if (count % 1000 == 0)
        makingProgress();
    }

    if (iter.hasNext())
      throw new Exception("index rebuild mismatch " + iter.next().getKey() + " " + tmpIndexTableName);

    log.debug("Verified " + count + " index entries ");

    state.getConnector().tableOperations().delete(indexTableName);
    state.getConnector().tableOperations().rename(tmpIndexTableName, indexTableName);
  }

}
