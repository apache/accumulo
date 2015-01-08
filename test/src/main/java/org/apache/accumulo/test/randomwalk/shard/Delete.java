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

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class Delete extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    String indexTableName = (String) state.get("indexTableName");
    String dataTableName = (String) state.get("docTableName");
    int numPartitions = (Integer) state.get("numPartitions");
    Random rand = (Random) state.get("rand");

    Entry<Key,Value> entry = Search.findRandomDocument(state, dataTableName, rand);
    if (entry == null)
      return;

    String docID = entry.getKey().getRow().toString();
    String doc = entry.getValue().toString();

    Insert.unindexDocument(state.getMultiTableBatchWriter().getBatchWriter(indexTableName), doc, docID, numPartitions);

    Mutation m = new Mutation(docID);
    m.putDelete("doc", "");

    state.getMultiTableBatchWriter().getBatchWriter(dataTableName).addMutation(m);

    log.debug("Deleted document " + docID);

    state.getMultiTableBatchWriter().flush();
  }

}
