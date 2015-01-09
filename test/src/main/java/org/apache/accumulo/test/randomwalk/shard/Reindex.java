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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class Reindex extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    String indexTableName = (String) state.get("indexTableName");
    String tmpIndexTableName = indexTableName + "_tmp";
    String docTableName = (String) state.get("docTableName");
    int numPartitions = (Integer) state.get("numPartitions");

    Random rand = (Random) state.get("rand");

    ShardFixture.createIndexTable(this.log, state, "_tmp", rand);

    Scanner scanner = state.getConnector().createScanner(docTableName, Authorizations.EMPTY);
    BatchWriter tbw = state.getConnector().createBatchWriter(tmpIndexTableName, new BatchWriterConfig());

    int count = 0;

    for (Entry<Key,Value> entry : scanner) {
      String docID = entry.getKey().getRow().toString();
      String doc = entry.getValue().toString();

      Insert.indexDocument(tbw, doc, docID, numPartitions);

      count++;
    }

    tbw.close();

    log.debug("Reindexed " + count + " documents into " + tmpIndexTableName);

  }

}
