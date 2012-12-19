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
package org.apache.accumulo.core.cli;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import com.beust.jcommander.JCommander;

public class TestClientOpts {
  
  @Test
  public void test() {
    BatchWriterConfig cfg = new BatchWriterConfig();
    
    // document the defaults
    ClientOpts args = new ClientOpts();
    assertEquals(System.getProperty("user.name"), args.user);
    assertNull(args.securePassword);
    assertArrayEquals("secret".getBytes(), args.getPassword());
    assertEquals(new Long(cfg.getMaxLatency(TimeUnit.MILLISECONDS)), args.batchLatency);
    assertEquals(new Long(cfg.getTimeout(TimeUnit.MILLISECONDS)), args.batchTimeout);
    assertEquals(new Long(cfg.getMaxMemory()), args.batchMemory);
    assertFalse(args.debug);
    assertFalse(args.trace);
    assertEquals(10, args.scanThreads.intValue());
    assertEquals(null, args.instance);
    assertEquals(Constants.NO_AUTHS, args.auths);
    assertEquals("localhost:2181", args.zookeepers);
    assertFalse(args.help);

    args = evaluate(
        "-u", "bar",
        "-p", "foo",
        "--batchLatency", "3s", 
        "--batchTimeout", "2s", 
        "--batchMemory", "1M",
        "--debug", 
        "--trace", 
        "--scanThreads", "7",
        "-i", "instance",
        "--auths", "G1,G2,G3",
        "-z", "zoohost1,zoohost2",
        "--help"
        );
    assertEquals("bar", args.user);
    assertNull(args.securePassword);
    assertArrayEquals("foo".getBytes(), args.getPassword());
    assertEquals(new Long(3000), args.batchLatency);
    assertEquals(new Long(2000), args.batchTimeout);
    assertEquals(new Long(1024*1024), args.batchMemory);
    assertTrue(args.debug);
    assertTrue(args.trace);
    assertEquals(7, args.scanThreads.intValue());
    assertEquals("instance", args.instance);
    assertEquals(new Authorizations("G1", "G2", "G3"), args.auths);
    assertEquals("zoohost1,zoohost2", args.zookeepers);
    assertTrue(args.help);
    
  }

  private ClientOpts evaluate(String ... args) {
    ClientOpts ca = new ClientOpts();
    new JCommander(ca, args);
    return ca;
  }
  
}
