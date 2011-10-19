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
package org.apache.accumulo.core.iterators.user;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IntersectingIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class IntersectingIteratorTest2 {
    
    @Test
    public void test() throws Exception {
        Value empty = new Value(new byte[] {});
        MockInstance inst = new MockInstance("mockabye");
        Connector connector = inst.getConnector("user", "pass");
        connector.tableOperations().create("index");
        BatchWriter bw = connector.createBatchWriter("index", 1000, 1000, 1);
        Mutation m = new Mutation("000012");
        m.put("rvy", "5000000000000000", empty);
        m.put("15qh", "5000000000000000", empty);
        bw.addMutation(m);
        bw.close();
        
        BatchScanner bs = connector.createBatchScanner("index", Constants.NO_AUTHS, 10);
        IteratorSetting ii = new IteratorSetting(20, IntersectingIterator.class);
        IntersectingIterator.setColumnFamilies(ii, new Text[] {new Text("rvy"), new Text("15qh")});
        bs.addScanIterator(ii);
        bs.setRanges(Collections.singleton(new Range()));
        Iterator<Entry<Key,Value>> iterator = bs.iterator();
        Assert.assertTrue(iterator.hasNext());
        Entry<Key,Value> next = iterator.next();
        Key key = next.getKey();
        Assert.assertEquals(key.getColumnQualifier(), new Text("5000000000000000"));
        Assert.assertFalse(iterator.hasNext());
    }
    
}
