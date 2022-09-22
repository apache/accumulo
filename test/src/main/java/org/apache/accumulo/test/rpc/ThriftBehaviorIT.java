/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.rpc;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.test.rpc.thrift.SimpleThriftService;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag(SUNNY_DAY)
@Timeout(value = 5, unit = SECONDS)
public class ThriftBehaviorIT extends WithTestNames {

  private SimpleThriftService.Client client;
  private SimpleThriftServiceHandler handler;
  private SimpleThriftServiceRunner serviceRunner;
  private String propName;

  private static final String KITTY_MSG = "🐈 Kitty! 🐈";

  @BeforeEach
  public void createClientAndServer() {
    String threadName = ThriftBehaviorIT.class.getSimpleName() + "." + testName();
    serviceRunner = new SimpleThriftServiceRunner(threadName);
    serviceRunner.startService();
    client = serviceRunner.client();
    handler = serviceRunner.handler();

    propName = testName();
    if (propName.endsWith("Handler")) {
      propName = propName.substring(0, propName.length() - 7);
    }
    propName = SimpleThriftServiceHandler.class.getSimpleName() + "." + propName;

    // make sure the property is reset before the test runs
    System.setProperty(propName, "-");
    assertEquals("-", System.getProperty(propName));
  }

  @AfterEach
  public void shutdownServer() {
    serviceRunner.stopService();

    // make sure the method was actually executed by the service handler
    assertEquals(KITTY_MSG, System.getProperty(propName));
  }

  @Test
  public void echoFailHandler() throws TException {
    var e = assertThrows(TException.class, () -> handler.echoFail(KITTY_MSG));
    assertTrue(e.getCause() instanceof UnsupportedOperationException);
  }

  @Test
  public void echoFail() throws TException {
    assertThrows(TApplicationException.class, () -> client.echoFail(KITTY_MSG));
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void echoRuntimeFailHandler() {
    assertThrows(UnsupportedOperationException.class, () -> handler.echoRuntimeFail(KITTY_MSG));
  }

  @Test
  public void echoRuntimeFail() throws TException {
    assertThrows(TApplicationException.class, () -> client.echoRuntimeFail(KITTY_MSG));
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void echoPassHandler() {
    assertEquals(KITTY_MSG, handler.echoPass(KITTY_MSG));
  }

  @Test
  public void echoPass() throws TException {
    assertEquals(KITTY_MSG, client.echoPass(KITTY_MSG));
  }

  @Test
  public void onewayFailHandler() throws TException {
    var e = assertThrows(TException.class, () -> handler.onewayFail(KITTY_MSG));
    assertTrue(e.getCause() instanceof UnsupportedOperationException);
  }

  @Test
  public void onewayFail() throws TException {
    client.onewayFail(KITTY_MSG);
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void onewayRuntimeFailHandler() {
    assertThrows(UnsupportedOperationException.class, () -> handler.onewayRuntimeFail(KITTY_MSG));
  }

  @Test
  public void onewayRuntimeFail() throws TException {
    client.onewayRuntimeFail(KITTY_MSG);
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void onewayPassHandler() {
    handler.onewayPass(KITTY_MSG);
  }

  @Test
  public void onewayPass() throws TException {
    client.onewayPass(KITTY_MSG);
    // verify normal two-way method still passes using same client
    echoPass();
  }

}
