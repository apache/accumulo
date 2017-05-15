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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.SampleNotPresentException;
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

@Deprecated
public class MockScannerTest {

  public static final String ROOT = "root";
  public static final String TEST = "test";
  public static final String SEP = ",";
  public static final String A_B_C_D = 'A' + SEP + 'B' + SEP + 'C' + SEP + 'D';
  public static final String CF = "cf";
  public static final String CQ = "cq";

  public static class DeepCopyIterator extends WrappingIterator {

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      super.init(source.deepCopy(env), options, env);
    }
  }

  @Test
  public void testDeepCopy() throws Exception {
    MockInstance inst = new MockInstance();
    Connector conn = inst.getConnector(ROOT, new PasswordToken(""));
    conn.tableOperations().create(TEST);
    BatchWriter bw = conn.createBatchWriter(TEST, new BatchWriterConfig());
    for (String row : A_B_C_D.split(SEP)) {
      Mutation m = new Mutation(row);
      m.put(CF, CQ, "");
      bw.addMutation(m);
    }
    bw.flush();
    BatchScanner bs = conn.createBatchScanner(TEST, Authorizations.EMPTY, 1);
    IteratorSetting cfg = new IteratorSetting(100, DeepCopyIterator.class);
    bs.addScanIterator(cfg);
    bs.setRanges(Collections.singletonList(new Range("A", "Z")));
    StringBuilder sb = new StringBuilder();
    String comma = "";
    for (Entry<Key,Value> entry : bs) {
      sb.append(comma);
      sb.append(entry.getKey().getRow());
      comma = SEP;
    }
    assertEquals(A_B_C_D, sb.toString());
  }

  @Test
  public void testEnvironment() throws Exception {
    MockScannerBase.MockIteratorEnvironment env = new MockScannerBase.MockIteratorEnvironment(Authorizations.EMPTY);
    assertFalse(env.isSamplingEnabled());
    assertNull(env.getSamplerConfiguration());
    try {
      env.cloneWithSamplingEnabled();
      fail("cloneWithSamplingEnabled should have thrown SampleNotPresentException");
    } catch (SampleNotPresentException se) {
      // expected
    }
  }

}
