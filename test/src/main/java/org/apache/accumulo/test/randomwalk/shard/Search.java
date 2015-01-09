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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Search extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    String indexTableName = (String) state.get("indexTableName");
    String dataTableName = (String) state.get("docTableName");

    Random rand = (Random) state.get("rand");

    Entry<Key,Value> entry = findRandomDocument(state, dataTableName, rand);
    if (entry == null)
      return;

    Text docID = entry.getKey().getRow();
    String doc = entry.getValue().toString();

    String[] tokens = doc.split("\\W+");
    int numSearchTerms = rand.nextInt(6);
    if (numSearchTerms < 2)
      numSearchTerms = 2;

    HashSet<String> searchTerms = new HashSet<String>();
    while (searchTerms.size() < numSearchTerms)
      searchTerms.add(tokens[rand.nextInt(tokens.length)]);

    Text columns[] = new Text[searchTerms.size()];
    int index = 0;
    for (String term : searchTerms) {
      columns[index++] = new Text(term);
    }

    log.debug("Looking up terms " + searchTerms + " expect to find " + docID);

    BatchScanner bs = state.getConnector().createBatchScanner(indexTableName, Authorizations.EMPTY, 10);
    IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
    IntersectingIterator.setColumnFamilies(ii, columns);
    bs.addScanIterator(ii);
    bs.setRanges(Collections.singleton(new Range()));

    boolean sawDocID = false;

    for (Entry<Key,Value> entry2 : bs) {
      if (entry2.getKey().getColumnQualifier().equals(docID)) {
        sawDocID = true;
        break;
      }
    }

    bs.close();

    if (!sawDocID)
      throw new Exception("Did not see doc " + docID + " in index.  terms:" + searchTerms + " " + indexTableName + " " + dataTableName);
  }

  static Entry<Key,Value> findRandomDocument(State state, String dataTableName, Random rand) throws Exception {
    Scanner scanner = state.getConnector().createScanner(dataTableName, Authorizations.EMPTY);
    scanner.setBatchSize(1);
    scanner.setRange(new Range(Integer.toString(rand.nextInt(0xfffffff), 16), null));

    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (!iter.hasNext())
      return null;

    return iter.next();
  }

}
