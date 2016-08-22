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
package org.apache.accumulo.core.client.mock;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

@Deprecated
public class TestBatchScanner821 {

  public static class TransformIterator extends WrappingIterator {

    @Override
    public Key getTopKey() {
      Key k = getSource().getTopKey();
      return new Key(new Text(k.getRow().toString().toLowerCase()), k.getColumnFamily(), k.getColumnQualifier(), k.getColumnVisibility(), k.getTimestamp());
    }
  }

  @Test
  public void test() throws Exception {
    MockInstance inst = new MockInstance();
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
    for (String row : "A,B,C,D".split(",")) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", "");
      bw.addMutation(m);
    }
    bw.flush();
    BatchScanner bs = conn.createBatchScanner("test", Authorizations.EMPTY, 1);
    IteratorSetting cfg = new IteratorSetting(100, TransformIterator.class);
    bs.addScanIterator(cfg);
    bs.setRanges(Collections.singletonList(new Range("A", "Z")));
    StringBuilder sb = new StringBuilder();
    String comma = "";
    for (Entry<Key,Value> entry : bs) {
      sb.append(comma);
      sb.append(entry.getKey().getRow());
      comma = ",";
    }
    assertEquals("a,b,c,d", sb.toString());
  }

}
