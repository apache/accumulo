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
package org.apache.accumulo.server.logger;

import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.server.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.server.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.server.logger.LogEvents.MUTATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.LogFile;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.ZooConfiguration;
import org.apache.accumulo.server.logger.LogEvents;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.accumulo.server.logger.LogWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLogWriter {
  
  private static final AuthInfo CREDENTIALS = new AuthInfo();
  
  static final String INSTANCE_ID = "SomeInstance123";
  static FileSystem fs = null;
  
  LogWriter writer;
  
  @Before
  public void setUp() throws Exception {
    // suppress log messages having to do with not having an instance
    Logger.getLogger(ZooConfiguration.class).setLevel(Level.OFF);
    Logger.getLogger(HdfsZooInstance.class).setLevel(Level.OFF);
    if (fs == null) {
      fs = FileSystem.getLocal(CachedConfiguration.getInstance());
    }
    writer = new LogWriter(ServerConfiguration.getDefaultConfiguration(), fs, Collections.singletonList("target"), INSTANCE_ID, 1, false);
    Logger.getLogger(LogWriter.class).setLevel(Level.FATAL);
  }
  
  @After
  public void tearDown() throws Exception {
    writer.shutdown();
    writer = null;
  }
  
  @Test
  public void testClose() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    writer.close(null, logFile.id);
    try {
      writer.close(null, logFile.id);
      fail("close didn't throw exception");
    } catch (NoSuchLogIDException ex) {
      // ignored
    }
    cleanup(logFile);
  }
  
  private void cleanup(LogFile logFile) throws Exception {
    new File("./" + logFile.name).delete();
  }
  
  @Test
  public void testCopy() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    Path mylog = new Path("mylog");
    fs.delete(mylog, true);
    assertTrue(!fs.exists(mylog));
    writer.startCopy(null, CREDENTIALS, logFile.name, "mylog", false);
    for (int i = 0; i < 100; i++) {
      UtilWaitThread.sleep(100);
      if (fs.exists(mylog))
        break;
    }
    assertTrue(fs.exists(mylog));
    Mutation m = new Mutation(new Text("row1"));
    m.put(new Text("cf"), new Text("cq"), new Value("value".getBytes()));
    try {
      writer.log(null, logFile.id, 1, 0, m.toThrift());
      fail("writing to a log after it has been copied to hdfs should fail");
    } catch (NoSuchLogIDException ex) {
      // ignored
    }
    fs.delete(mylog, true);
    cleanup(logFile);
  }
  
  private SequenceFile.Reader readOpen(LogFile logFile) throws Exception {
    String path = "./target/" + logFile.name;
    assertTrue(fs.exists(new Path(path)));
    SequenceFile.Reader result = new SequenceFile.Reader(fs, new Path(path), fs.getConf());
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(result.next(key, value));
    assertTrue(key.event == LogEvents.OPEN);
    assertTrue(key.tid == LogFileKey.VERSION);
    return result;
  }
  
  @Test
  public void testCreate() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    writer.close(null, logFile.id);
    readOpen(logFile);
    cleanup(logFile);
  }
  
  @Test
  public void testLog() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    Mutation m = new Mutation(new Text("somerow"));
    m.put(new Text("cf1"), new Text("cq1"), new Value("value1".getBytes()));
    writer.log(null, logFile.id, 2, 42, m.toThrift());
    writer.close(null, logFile.id);
    SequenceFile.Reader dis = readOpen(logFile);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(dis.next(key, value));
    assertTrue(key.event == MUTATION);
    assertEquals(key.seq, 2);
    assertEquals(key.tid, 42);
    assertEquals(value.mutations.length, 1);
    assertTrue(m.equals(value.mutations[0]));
    cleanup(logFile);
  }
  
  @Test
  public void testLogManyTablets() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    List<TMutation> all = new ArrayList<TMutation>();
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation(new Text("somerow"));
      m.put(new Text("cf" + i), new Text("cq" + i), new Value(("value" + i).getBytes()));
      all.add(m.toThrift());
    }
    List<TabletMutations> updates = new ArrayList<TabletMutations>();
    updates.add(new TabletMutations(13, 3, all));
    writer.logManyTablets(null, logFile.id, updates);
    writer.close(null, logFile.id);
    SequenceFile.Reader dis = readOpen(logFile);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(dis.next(key, value));
    assertTrue(key.event == MANY_MUTATIONS);
    assertEquals(key.seq, 3);
    assertEquals(key.tid, 13);
    assertEquals(value.mutations.length, 10);
    for (int i = 0; i < 10; i++) {
      assertTrue(new Mutation(all.get(i)).equals(value.mutations[i]));
    }
    cleanup(logFile);
  }
  
  @Test
  public void testMinorCompactionFinished() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    String fqfn = "/foo/bar";
    writer.minorCompactionFinished(null, logFile.id, 4, 17, fqfn);
    writer.close(null, logFile.id);
    SequenceFile.Reader dis = readOpen(logFile);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(dis.next(key, value));
    assertTrue(key.event == COMPACTION_FINISH);
    assertEquals(key.seq, 4);
    assertEquals(key.tid, 17);
    cleanup(logFile);
  }
  
  @Test
  public void testMinorCompactionStarted() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    String fqfn = "/foo/bar";
    writer.minorCompactionStarted(null, logFile.id, 5, 23, fqfn);
    writer.close(null, logFile.id);
    SequenceFile.Reader dis = readOpen(logFile);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(dis.next(key, value));
    assertTrue(key.event == COMPACTION_START);
    assertEquals(key.seq, 5);
    assertEquals(key.tid, 23);
    assertEquals(key.filename, "/foo/bar");
    cleanup(logFile);
  }
  
  @Test
  public void testDefineTablet() throws Exception {
    LogFile logFile = writer.create(null, CREDENTIALS, "");
    KeyExtent ke = new KeyExtent(new Text("table1"), new Text("zzzz"), new Text("aaaaa"));
    writer.defineTablet(null, logFile.id, 6, 31, ke.toThrift());
    writer.close(null, logFile.id);
    SequenceFile.Reader dis = readOpen(logFile);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();
    assertTrue(dis.next(key, value));
    assertTrue(key.event == DEFINE_TABLET);
    assertEquals(key.seq, 6);
    assertEquals(key.tid, 31);
    assertEquals(ke, key.tablet);
  }
  
  @Test
  public void testNothing() {}
}
