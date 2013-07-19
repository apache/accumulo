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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class ClassLoaderIT extends SimpleMacIT {
  
  @Test(timeout=60*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());
    Mutation m = new Mutation("row1");
    m.put("cf", "col1", "Test");
    bw.addMutation(m);
    bw.close();
    scanCheck(c, "Test");
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Path jarPath = new Path(rootPath() + "/lib/Test.jar");
    fs.copyFromLocalFile(new Path(System.getProperty("user.dir")+"/system/auto/TestCombinerX.jar"), jarPath);
    UtilWaitThread.sleep(1000);
    IteratorSetting is = new IteratorSetting(10, "TestCombiner", "org.apache.accumulo.test.functional.TestCombiner");
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf")));
    c.tableOperations().attachIterator("test", is, EnumSet.of(IteratorScope.scan));
    UtilWaitThread.sleep(5000);
    scanCheck(c, "TestX");
    fs.delete(jarPath, true);
    fs.copyFromLocalFile(new Path(System.getProperty("user.dir")+"/system/auto/TestCombinerY.jar"), jarPath);
    UtilWaitThread.sleep(5000);
    scanCheck(c, "TestY");
  }

  private void scanCheck(Connector c, String expected) throws Exception {
    Scanner bs = c.createScanner("test", Authorizations.EMPTY);
    Iterator<Entry<Key,Value>> iterator = bs.iterator();
    assertTrue(iterator.hasNext());
    Entry<Key,Value> next = iterator.next();
    assertFalse(iterator.hasNext());
    assertEquals(expected, next.getValue().toString());
  }

}
