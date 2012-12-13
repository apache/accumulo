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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AccumuloFileOutputFormatTest {
  static Job job;
  static TaskAttemptContext tac;
  static Path f = null;
  
  @Before
  public void setup() throws IOException {
    job = new Job();
    
    Path file = new Path("target/");
    f = new Path(file, "_temporary");
    job.getConfiguration().set("mapred.output.dir", file.toString());
    
    tac = ContextFactory.createTaskAttemptContext(job);
  }
  
  @After
  public void teardown() throws IOException {
    if (f != null && f.getFileSystem(job.getConfiguration()).exists(f)) {
      f.getFileSystem(job.getConfiguration()).delete(f, true);
    }
  }
  
  @Test
  public void testEmptyWrite() throws IOException, InterruptedException {
    handleWriteTests(false);
  }
  
  @Test
  public void testRealWrite() throws IOException, InterruptedException {
    handleWriteTests(true);
  }
  
  public void handleWriteTests(boolean content) throws IOException, InterruptedException {
    AccumuloFileOutputFormat afof = new AccumuloFileOutputFormat();
    RecordWriter<Key,Value> rw = afof.getRecordWriter(tac);
    
    if (content)
      rw.write(new Key("Key"), new Value("".getBytes()));
    
    Path file = afof.getDefaultWorkFile(tac, ".rf");
    System.out.println(file);
    rw.close(tac);
    
    if (content)
      assertTrue(file.getFileSystem(job.getConfiguration()).exists(file));
    else
      assertFalse(file.getFileSystem(job.getConfiguration()).exists(file));
    file.getFileSystem(tac.getConfiguration()).delete(file.getParent(), true);
  }
  
  @Test
  public void validateConfiguration() throws IOException, InterruptedException {
    
    int a = 7;
    long b = 300l;
    long c = 50l;
    long d = 10l;
    String e = "type";
    
    AccumuloFileOutputFormat.setReplication(job, a);
    AccumuloFileOutputFormat.setFileBlockSize(job, b);
    AccumuloFileOutputFormat.setDataBlockSize(job, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job, d);
    AccumuloFileOutputFormat.setCompressionType(job, e);
    
    AccumuloConfiguration acuconf = AccumuloFileOutputFormat.getAccumuloConfiguration(job);
    
    assertEquals(a, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(b, acuconf.getMemoryInBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(c, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(d, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals(e, acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
  }
}
