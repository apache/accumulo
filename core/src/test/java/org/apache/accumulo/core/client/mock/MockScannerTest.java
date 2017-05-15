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

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@Deprecated
public class MockScannerTest {

  public static class DeepCopyIterator extends WrappingIterator {

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      super.init(source.deepCopy(env), options, env);
    }
  }

  @Test
  public void testDeepCopy() throws Exception {
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
    IteratorSetting cfg = new IteratorSetting(100, DeepCopyIterator.class);
    bs.addScanIterator(cfg);
    bs.setRanges(Collections.singletonList(new Range("A", "Z")));
    StringBuilder sb = new StringBuilder();
    String comma = "";
    for (Entry<Key,Value> entry : bs) {
      sb.append(comma);
      sb.append(entry.getKey().getRow());
      comma = ",";
    }
    assertEquals("A,B,C,D", sb.toString());
  }

  @Test
  public void testEnvironment() throws Exception {
    MockScannerBase.MockIteratorEnvironment env = new MockScannerBase.MockIteratorEnvironment(Authorizations.EMPTY);
    assertFalse(env.isSamplingEnabled());
    assertNull(env.getSamplerConfiguration());
    try {
      env.cloneWithSamplingEnabled();
    } catch (SampleNotPresentException se) {
      // expected
    }
  }

}
