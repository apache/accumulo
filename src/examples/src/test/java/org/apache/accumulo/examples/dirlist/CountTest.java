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
package org.apache.accumulo.examples.dirlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.examples.dirlist.FileCount;
import org.apache.accumulo.examples.dirlist.FileCountMR;
import org.apache.accumulo.examples.dirlist.Ingest;
import org.apache.accumulo.examples.dirlist.QueryUtil;
import org.apache.accumulo.examples.dirlist.StringArraySummation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class CountTest extends TestCase {
  {
    try {
      Connector conn = new MockInstance("counttest").getConnector("root", "".getBytes());
      conn.tableOperations().create("dirlisttable");
      BatchWriter bw = conn.createBatchWriter("dirlisttable", 1000000l, 100l, 1);
      ColumnVisibility cv = new ColumnVisibility();
      // / has 1 dir
      // /local has 2 dirs 1 file
      // /local/user1 has 2 files
      bw.addMutation(Ingest.buildMutation(cv, "/local", true, false, true, 272, 12345, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/user1", true, false, true, 272, 12345, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/user2", true, false, true, 272, 12345, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 12345, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 23456, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file1", false, false, false, 2024, 12345, null));
      bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file2", false, false, false, 1028, 23456, null));
      bw.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static class AggregatingMap extends TreeMap<Key,Value> {
    private static final long serialVersionUID = -6644406149713336633L;
    private Aggregator agg;
    
    public AggregatingMap(Aggregator agg) {
      this.agg = agg;
    }
    
    @Override
    public Value put(Key key, Value value) {
      if (!this.containsKey(key)) return super.put(key, value);
      agg.reset();
      agg.collect(value);
      agg.collect(this.get(key));
      return super.put(key, agg.aggregate());
    }
  }
  
  public void test() throws Exception {
    JobContext job = new JobContext(new Configuration(), new JobID());
    AccumuloInputFormat.setInputInfo(job, "root", "".getBytes(), "dirlisttable", new Authorizations());
    AccumuloInputFormat.setMockInstance(job, "counttest");
    AccumuloInputFormat cif = new AccumuloInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
    RecordReader<Key,Value> rr = cif.createRecordReader(ris, tac);
    rr.initialize(ris, tac);
    FileCountMR.FileCountMapper mapper = new FileCountMR.FileCountMapper();
    RecordWriter<Key,Value> rw = new RecordWriter<Key,Value>() {
      Map<Key,Value> aggmap = new AggregatingMap(new StringArraySummation());
      
      @Override
      public void write(Key key, Value value) throws IOException, InterruptedException {
        aggmap.put(key, value);
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        ArrayList<Pair<String,String>> expected = new ArrayList<Pair<String,String>>();
        expected.add(new Pair<String,String>("", "1,0,3,3"));
        expected.add(new Pair<String,String>("/local", "2,1,2,3"));
        expected.add(new Pair<String,String>("/local/user1", "0,2,0,2"));
        
        int i = 0;
        for (Entry<Key,Value> e : aggmap.entrySet()) {
          assertEquals(e.getKey().getRow().toString(), expected.get(i).getFirst());
          assertEquals(e.getValue().toString(), expected.get(i).getSecond());
          i++;
        }
        assertEquals(aggmap.entrySet().size(), expected.size());
      }
    };
    Mapper<Key,Value,Key,Value>.Context context = mapper.new Context(job.getConfiguration(), new TaskAttemptID(), rr, rw, null, null, ris);
    mapper.run(context);
    rw.close(context);
  }
  
  public void test2() throws Exception {
    Scanner scanner = new MockInstance("counttest").getConnector("root", "".getBytes()).createScanner("dirlisttable", new Authorizations());
    scanner.fetchColumn(new Text("dir"), new Text("counts"));
    assertFalse(scanner.iterator().hasNext());
    
    FileCount fc = new FileCount("counttest", null, "root", "", "dirlisttable", "", "", true);
    fc.run();
    
    ArrayList<Pair<String,String>> expected = new ArrayList<Pair<String,String>>();
    expected.add(new Pair<String,String>(QueryUtil.getRow("").toString(), "1,0,3,3"));
    expected.add(new Pair<String,String>(QueryUtil.getRow("/local").toString(), "2,1,2,3"));
    expected.add(new Pair<String,String>(QueryUtil.getRow("/local/user1").toString(), "0,2,0,2"));
    expected.add(new Pair<String,String>(QueryUtil.getRow("/local/user2").toString(), "0,0,0,0"));
    
    int i = 0;
    for (Entry<Key,Value> e : scanner) {
      assertEquals(e.getKey().getRow().toString(), expected.get(i).getFirst());
      assertEquals(e.getValue().toString(), expected.get(i).getSecond());
      i++;
    }
    assertEquals(i, expected.size());
  }
}
