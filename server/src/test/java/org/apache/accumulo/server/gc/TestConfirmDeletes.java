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
package org.apache.accumulo.server.gc;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.gc.SimpleGarbageCollector.Opts;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestConfirmDeletes {
  
  SortedSet<String> newSet(String... s) {
    SortedSet<String> result = new TreeSet<String>(Arrays.asList(s));
    return result;
  }
  
  @Test
  public void test() throws Exception {
    
    // have a directory reference
    String metadata[] = {"1636< last:3353986642a66eb 192.168.117.9:9997", "1636< srv:dir /default_tablet", "1636< srv:flush 2",
        "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb", "1636< srv:time M1328505870023", "1636< ~tab:~pr \0"};
    String deletes[] = {"~del/1636/default_tablet"};
    
    test1(metadata, deletes, 1, 0);
    
    // have no file reference
    deletes = new String[] {"~del/1636/default_tablet/someFile"};
    test1(metadata, deletes, 1, 1);
    
    // have a file reference
    metadata = new String[] {"1636< file:/default_tablet/someFile 10,100", "1636< last:3353986642a66eb 192.168.117.9:9997", "1636< srv:dir /default_tablet",
        "1636< srv:flush 2", "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb", "1636< srv:time M1328505870023",
        "1636< ~tab:~pr \0"};
    test1(metadata, deletes, 1, 0);
    
    // have an indirect file reference
    deletes = new String[] {"~del/9/default_tablet/someFile"};
    metadata = new String[] {"1636< file:../9/default_tablet/someFile 10,100", "1636< last:3353986642a66eb 192.168.117.9:9997",
        "1636< srv:dir /default_tablet", "1636< srv:flush 2", "1636< srv:lock tservers/192.168.117.9:9997/zlock-0000000000$3353986642a66eb",
        "1636< srv:time M1328505870023", "1636< ~tab:~pr \0"};
    
    test1(metadata, deletes, 1, 0);
    
    // have an indirect file reference and a directory candidate
    deletes = new String[] {"~del/9/default_tablet"};
    test1(metadata, deletes, 1, 0);
    
    deletes = new String[] {"~del/9/default_tablet", "~del/9/default_tablet/someFile"};
    test1(metadata, deletes, 2, 0);
    
    deletes = new String[] {"~blip/1636/b-0001", "~del/1636/b-0001/I0000"};
    test1(metadata, deletes, 1, 0);
  }
  
  private void test1(String[] metadata, String[] deletes, int expectedInitial, int expected) throws Exception {
    Credentials credentials = new Credentials("root", new PasswordToken(new byte[0]));
    
    Instance instance = new MockInstance();
    VolumeManager fs = VolumeManagerImpl.getLocal();
    
    load(instance, metadata, deletes);
    
    SimpleGarbageCollector gc = new SimpleGarbageCollector(new Opts());
    gc.init(fs, instance, credentials, false);
    SortedSet<String> candidates = gc.getCandidates();
    Assert.assertEquals(expectedInitial, candidates.size());
    gc.confirmDeletes(candidates);
    Assert.assertEquals(expected, candidates.size());
  }
  
  private void load(Instance instance, String[] metadata, String[] deletes) throws Exception {
    Credentials credentials = new Credentials("root", new PasswordToken(new byte[0]));
    
    Scanner scanner = instance.getConnector(credentials.getPrincipal(), credentials.getToken()).createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    // ensure there is no data from previous test
    Assert.assertEquals(0, count);
    
    Connector conn = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    for (String line : metadata) {
      String[] parts = line.split(" ");
      String[] columnParts = parts[1].split(":");
      Mutation m = new Mutation(parts[0]);
      m.put(new Text(columnParts[0]), new Text(columnParts[1]), new Value(parts[2].getBytes()));
      bw.addMutation(m);
    }
    
    for (String line : deletes) {
      Mutation m = new Mutation(line);
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
  }
}
