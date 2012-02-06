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
package org.apache.accumulo.examples.wikisearch.ingest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.examples.wikisearch.reader.AggregatingRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class WikipediaInputFormat extends TextInputFormat {

  public static class WikipediaInputSplit extends InputSplit implements Writable {

    public WikipediaInputSplit(){}
    
    public WikipediaInputSplit(FileSplit fileSplit, int partition)
    {
      this.fileSplit = fileSplit;
      this.partition = partition;
    }
    
    private FileSplit fileSplit = null;
    private int partition = -1;

    public int getPartition()
    {
      return partition;
    }
    
    public FileSplit getFileSplit()
    {
      return fileSplit;
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
      return fileSplit.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return fileSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      Path file = new Path(in.readUTF());
      long start = in.readLong();
      long length = in.readLong();
      int numHosts = in.readInt();
      String[] hosts = new String[numHosts];
      for(int i = 0; i < numHosts; i++)
        hosts[i] = in.readUTF();
      fileSplit = new FileSplit(file, start, length, hosts);
      partition = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(fileSplit.getPath().toString());
      out.writeLong(fileSplit.getStart());
      out.writeLong(fileSplit.getLength());
      String [] hosts = fileSplit.getLocations();
      out.writeInt(hosts.length);
      for(String host:hosts)
        out.writeUTF(host);
      fileSplit.write(out);
      out.writeInt(partition);
    }
    
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> superSplits = super.getSplits(job);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    int numGroups = WikipediaConfiguration.getNumGroups(job.getConfiguration());

    for(InputSplit split:superSplits)
    {
      FileSplit fileSplit = (FileSplit)split;
      for(int group = 0; group < numGroups; group++)
      {
        splits.add(new WikipediaInputSplit(fileSplit,group));
      }
    }
    return splits;
  }

  @Override
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new AggregatingRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
}
