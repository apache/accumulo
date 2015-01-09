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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

//a test created to test the batch deleter
public class DeleteSomeDocs extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    // delete documents that where the document id matches a given pattern from doc and index table
    // using the batch deleter

    Random rand = (Random) state.get("rand");
    String indexTableName = (String) state.get("indexTableName");
    String dataTableName = (String) state.get("docTableName");

    ArrayList<String> patterns = new ArrayList<String>();

    for (Object key : props.keySet())
      if (key instanceof String && ((String) key).startsWith("pattern"))
        patterns.add(props.getProperty((String) key));

    String pattern = patterns.get(rand.nextInt(patterns.size()));
    BatchWriterConfig bwc = new BatchWriterConfig();
    BatchDeleter ibd = state.getConnector().createBatchDeleter(indexTableName, Authorizations.EMPTY, 8, bwc);
    ibd.setRanges(Collections.singletonList(new Range()));

    IteratorSetting iterSettings = new IteratorSetting(100, RegExFilter.class);
    RegExFilter.setRegexs(iterSettings, null, null, pattern, null, false);

    ibd.addScanIterator(iterSettings);

    ibd.delete();

    ibd.close();

    BatchDeleter dbd = state.getConnector().createBatchDeleter(dataTableName, Authorizations.EMPTY, 8, bwc);
    dbd.setRanges(Collections.singletonList(new Range()));

    iterSettings = new IteratorSetting(100, RegExFilter.class);
    RegExFilter.setRegexs(iterSettings, pattern, null, null, null, false);

    dbd.addScanIterator(iterSettings);

    dbd.delete();

    dbd.close();

    log.debug("Deleted documents w/ id matching '" + pattern + "'");
  }
}
