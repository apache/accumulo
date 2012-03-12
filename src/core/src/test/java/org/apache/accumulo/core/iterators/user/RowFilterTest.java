/**
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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * 
 */

public class RowFilterTest extends TestCase {
  
  public static class SummingRowFilter extends RowFilter {
    
    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      int sum = 0;
      int sum2 = 0;
      
      Key firstKey = null;
      
      if (rowIterator.hasTop()) {
        firstKey = new Key(rowIterator.getTopKey());
      }

      while (rowIterator.hasTop()) {
        sum += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }
      
      // ensure that seeks are confined to the row
      rowIterator.seek(new Range(), new HashSet<ByteSequence>(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }
      
      rowIterator.seek(new Range(firstKey.getRow(), false, null, true), new HashSet<ByteSequence>(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }
      
      return sum == 2 && sum2 == 2;
    }
    
  }

  public void test1() throws Exception {
    MockInstance instance = new MockInstance("rft1");
    Connector conn = instance.getConnector("", "".getBytes());
    
    conn.tableOperations().create("table1");
    BatchWriter bw = conn.createBatchWriter("table1", 1000000, 60000, 1);
    
    Mutation m = new Mutation("0");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    m.put("cf1", "cq3", "1");
    m.put("cf1", "cq4", "1");
    m.put("cf1", "cq5", "1");
    m.put("cf1", "cq6", "1");
    m.put("cf1", "cq7", "1");
    m.put("cf1", "cq8", "1");
    m.put("cf1", "cq9", "1");
    m.put("cf2", "cq1", "1");
    m.put("cf2", "cq2", "1");
    bw.addMutation(m);
    
    m = new Mutation("1");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "2");
    bw.addMutation(m);
    
    m = new Mutation("2");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    bw.addMutation(m);
    
    m = new Mutation("3");
    m.put("cf1", "cq1", "0");
    m.put("cf1", "cq2", "2");
    bw.addMutation(m);
    
    m = new Mutation("4");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    m.put("cf1", "cq3", "1");
    m.put("cf1", "cq4", "1");
    m.put("cf1", "cq5", "1");
    m.put("cf1", "cq6", "1");
    m.put("cf1", "cq7", "1");
    m.put("cf1", "cq8", "1");
    m.put("cf1", "cq9", "1");
    m.put("cf2", "cq1", "1");
    m.put("cf2", "cq2", "1");
    bw.addMutation(m);

    IteratorSetting is = new IteratorSetting(40, SummingRowFilter.class);
    conn.tableOperations().attachIterator("table1", is);

    Scanner scanner = conn.createScanner("table1", Constants.NO_AUTHS);
    assertEquals(new HashSet<String>(Arrays.asList("2", "3")), getRows(scanner));
    
    scanner.fetchColumn(new Text("cf1"), new Text("cq2"));
    assertEquals(new HashSet<String>(Arrays.asList("1", "3")), getRows(scanner));
    
    scanner.clearColumns();
    scanner.fetchColumn(new Text("cf1"), new Text("cq1"));
    assertEquals(new HashSet<String>(), getRows(scanner));
    
    scanner.setRange(new Range("0", "4"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(Arrays.asList("2", "3")), getRows(scanner));
    
    scanner.setRange(new Range("2"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(Arrays.asList("2")), getRows(scanner));
    
    scanner.setRange(new Range("4"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(), getRows(scanner));
    
    scanner.setRange(new Range("4"));
    scanner.clearColumns();
    scanner.fetchColumn(new Text("cf1"), new Text("cq2"));
    scanner.fetchColumn(new Text("cf1"), new Text("cq4"));
    assertEquals(new HashSet<String>(Arrays.asList("4")), getRows(scanner));

  }
  
  private HashSet<String> getRows(Scanner scanner) {
    HashSet<String> rows = new HashSet<String>();
    for (Entry<Key,Value> entry : scanner) {
      rows.add(entry.getKey().getRow().toString());
    }
    return rows;
  }
}
