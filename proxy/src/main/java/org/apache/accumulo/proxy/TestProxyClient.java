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
package org.apache.accumulo.proxy;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.PKey;
import org.apache.accumulo.proxy.thrift.PScanResult;
import org.apache.accumulo.proxy.thrift.PTimeType;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TestProxyClient {
  
  protected AccumuloProxy.Client proxy;
  protected TTransport transport;
  
  public TestProxyClient(String host, int port) throws TTransportException {
    final TSocket socket = new TSocket(host, port);
    socket.setTimeout(600000);
    transport = new TFramedTransport(socket);
    final TProtocol protocol = new TCompactProtocol(transport);
    proxy = new AccumuloProxy.Client(protocol);
    transport.open();
  }
  
  public AccumuloProxy.Client proxy() {
    return proxy;
  }
  
  public static void main(String[] args) throws Exception {
    
    TestProxyClient tpc = new TestProxyClient("localhost", 42424);
    UserPass userpass = new UserPass();
    userpass.setUsername("root");
    userpass.setPassword("secret".getBytes());
    
    System.out.println("Creating user: ");
    if (!tpc.proxy().securityOperations_listUsers(userpass).contains("testuser")) {
      tpc.proxy().securityOperations_createUser(userpass, "testuser", ByteBuffer.wrap("testpass".getBytes()));
    }
    System.out.println("UserList: " + tpc.proxy().securityOperations_listUsers(userpass));
    
    System.out.println("Pinging: " + tpc.proxy().ping(userpass));
    System.out.println("Listing: " + tpc.proxy().tableOperations_list(userpass));
    
    System.out.println("Deleting: ");
    String testTable = "testtableOMGOMGOMG";
    
    System.out.println("Creating: ");
    
    if (tpc.proxy().tableOperations_exists(userpass, testTable))
      tpc.proxy().tableOperations_delete(userpass, testTable);
    
    tpc.proxy().tableOperations_create(userpass, testTable, true, PTimeType.MILLIS);
    
    System.out.println("Listing: " + tpc.proxy().tableOperations_list(userpass));
    
    System.out.println("Writing: ");
    Date start = new Date();
    Date then = new Date();
    int maxInserts = 1000000;
    String format = "%1$05d";
    Map<ByteBuffer,List<PColumnUpdate>> mutations = new HashMap<ByteBuffer,List<PColumnUpdate>>();
    for (int i = 0; i < maxInserts; i++) {
      String result = String.format(format, i);
      PColumnUpdate update = new PColumnUpdate(ByteBuffer.wrap(("cf" + i).getBytes()), ByteBuffer.wrap(("cq" + i).getBytes()));
      update.setValue(Util.randStringBuffer(10));
      mutations.put(ByteBuffer.wrap(result.getBytes()), Collections.singletonList(update));
      
      if (i % 1000 == 0) {
        tpc.proxy().updateAndFlush(userpass, testTable, mutations);
        mutations.clear();
      }
    }
    tpc.proxy().updateAndFlush(userpass, testTable, mutations);
    Date end = new Date();
    System.out.println(" End of writing: " + (end.getTime() - start.getTime()));
    
    tpc.proxy().tableOperations_delete(userpass, testTable);
    tpc.proxy().tableOperations_create(userpass, testTable, true, PTimeType.MILLIS);
    
    // Thread.sleep(1000);
    
    System.out.println("Writing async: ");
    start = new Date();
    then = new Date();
    mutations.clear();
    String writer = tpc.proxy().createWriter(userpass, testTable);
    for (int i = 0; i < maxInserts; i++) {
      String result = String.format(format, i);
      PKey pkey = new PKey();
      pkey.setRow(result.getBytes());
      PColumnUpdate update = new PColumnUpdate(ByteBuffer.wrap(("cf" + i).getBytes()), ByteBuffer.wrap(("cq" + i).getBytes()));
      update.setValue(Util.randStringBuffer(10));
      mutations.put(ByteBuffer.wrap(result.getBytes()), Collections.singletonList(update));
      tpc.proxy().writer_update(writer, mutations);
      mutations.clear();
    }
    
    end = new Date();
    System.out.println(" End of writing: " + (end.getTime() - start.getTime()));
    start = end;
    System.out.println("Closing...");
    tpc.proxy().writer_close(writer);
    end = new Date();
    System.out.println(" End of closing: " + (end.getTime() - start.getTime()));
    
    System.out.println("Reading: ");
    
    String regex = "cf1.*";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, null, regex, null, null, false);
    
    PKey stop = new PKey();
    stop.setRow("5".getBytes());
    String cookie = tpc.proxy().createBatchScanner(userpass, testTable, null, null, null);
    
    int i = 0;
    start = new Date();
    then = new Date();
    boolean hasNext = true;
    
    int k = 1000;
    while (hasNext) {
      PScanResult kvList = tpc.proxy().scanner_next_k(cookie, k);
      
      Date now = new Date();
      System.out.println(i + " " + (now.getTime() - then.getTime()));
      then = now;
      
      i += kvList.getResultsSize();
      // for (TKeyValue kv:kvList.getResults()) System.out.println(new Key(kv.getKey()));
      hasNext = kvList.isMore();
    }
    end = new Date();
    System.out.println("Total entries: " + i + " total time " + (end.getTime() - start.getTime()));
  }
}
