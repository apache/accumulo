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
package org.apache.accumulo.server.gc;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;


/**
 * 
 */
public class TestConfirmDeletes {
  
  AuthInfo auth = new AuthInfo("root", ByteBuffer.wrap("secret".getBytes()), "instance");

  SortedSet<String> singletonSet(String s) {
    SortedSet<String> result = new TreeSet<String>();
    result.add(s);
    return result;
  }

  @Test
  public void test() throws Exception {
    
    // have a directory reference
    String metadata[] = {"1636< last:3353986642a66eb 192.168.117.9:9997", "1636< srv:dir /default_tablet", "1636< srv:flush 2",
        "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb", "1636< srv:time M1328505870023", "1636< ~tab:~pr \0",};
    
    SortedSet<String> candidates = singletonSet("/1636/default_tablet");
    test1(metadata, candidates);
    Assert.assertEquals(0, candidates.size());
    
    // have no file reference
    candidates = singletonSet("/1636/default_tablet/someFile");
    test1(metadata, candidates);
    Assert.assertEquals(1, candidates.size());
    
    // have a file reference
    metadata = new String[] {"1636< file:/default_tablet/someFile 10,100", "1636< last:3353986642a66eb 192.168.117.9:9997", "1636< srv:dir /default_tablet",
        "1636< srv:flush 2", "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb", "1636< srv:time M1328505870023",
        "1636< ~tab:~pr \0",};
    test1(metadata, candidates);
    Assert.assertEquals(0, candidates.size());
    
    // have an indirect file reference
    candidates = singletonSet("/1636/default_tablet/someFile");
    metadata = new String[] {"1636< file:../default_tablet/someFile 10,100", "1636< last:3353986642a66eb 192.168.117.9:9997", "1636< srv:dir /default_tablet",
        "1636< srv:flush 2", "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb", "1636< srv:time M1328505870023",
        "1636< ~tab:~pr \0",};
    test1(metadata, candidates);
    Assert.assertEquals(0, candidates.size());

  }
  
  private void test1(String[] metadata, SortedSet<String> candidates) throws Exception {
    Instance instance = new MockInstance("mockabyebaby");
    FileSystem fs = FileSystem.getLocal(CachedConfiguration.getInstance());
    AccumuloConfiguration aconf = DefaultConfiguration.getInstance();
    
    load(instance, metadata);

    SimpleGarbageCollector gc = new SimpleGarbageCollector(new String[] {});
    gc.init(fs, instance, auth, aconf);
    gc.confirmDeletes(candidates);
  }
  
  private void load(Instance instance, String[] metadata) throws Exception {
    Connector conn = instance.getConnector(auth);
    BatchWriter bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, 1000, 1000, 1);
    for (String line : metadata) {
      String[] parts = line.split(" ");
      String[] columnParts = parts[1].split(":");
      Mutation m = new Mutation(parts[0]);
      m.put(new Text(columnParts[0]), new Text(columnParts[1]), new Value(parts[2].getBytes()));
      bw.addMutation(m);
    }
    bw.close();
  }
}
