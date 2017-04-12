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
package org.apache.accumulo.test.rpc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.test.categories.SunnyDayTests;
import org.apache.accumulo.test.rpc.thrift.SimpleThriftService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.server.TSimpleServer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

@Category(SunnyDayTests.class)
public class ThriftBehaviorIT {

  @Rule
  public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private SimpleThriftService.Client client;
  private SimpleThriftServiceHandler handler;
  private SimpleThriftServiceRunner serviceRunner;
  private String propName;
  private Map<Logger,Level> oldLogLevels = new HashMap<>();

  private static final String KITTY_MSG = "🐈 Kitty! 🐈";

  // can delete wrapper when tests pass without using it (assuming tests are good enough)
  private static final boolean USE_RPC_WRAPPER = true;

  private static final boolean SUPPRESS_SPAMMY_LOGGERS = true;

  @Before
  public void createClientAndServer() {
    Arrays.stream(new Class<?>[] {TSimpleServer.class, ProcessFunction.class}).forEach(spammyClass -> {
      Logger spammyLogger = Logger.getLogger(spammyClass);
      oldLogLevels.put(spammyLogger, spammyLogger.getLevel());
      if (SUPPRESS_SPAMMY_LOGGERS) {
        spammyLogger.setLevel(Level.OFF);
      }
    });

    String threadName = ThriftBehaviorIT.class.getSimpleName() + "." + testName.getMethodName();
    serviceRunner = new SimpleThriftServiceRunner(threadName, USE_RPC_WRAPPER);
    serviceRunner.startService();
    client = serviceRunner.client();
    handler = serviceRunner.handler();

    propName = testName.getMethodName();
    if (propName.endsWith("Handler")) {
      propName = propName.substring(0, propName.length() - 7);
    }
    propName = SimpleThriftServiceHandler.class.getSimpleName() + "." + propName;

    // make sure the property is reset before the test runs
    System.setProperty(propName, "-");
    Assert.assertEquals("-", System.getProperty(propName));
  }

  @After
  public void shutdownServer() {
    serviceRunner.stopService();

    oldLogLevels.forEach((spammyLogger, oldLevel) -> {
      spammyLogger.setLevel(oldLevel);
    });

    // make sure the method was actually executed by the service handler
    Assert.assertEquals(KITTY_MSG, System.getProperty(propName));
  }

  @Test
  public void echoFailHandler() throws TException {
    exception.expect(TException.class);
    exception.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    handler.echoFail(KITTY_MSG);
  }

  @Test
  public void echoFail() throws TException {
    try {
      client.echoFail(KITTY_MSG);
      Assert.fail("Thrift client did not throw an expected exception");
    } catch (Exception e) {
      Assert.assertEquals(TApplicationException.class.getName(), e.getClass().getName());
    }
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void echoRuntimeFailHandler() throws TException {
    exception.expect(UnsupportedOperationException.class);
    handler.echoRuntimeFail(KITTY_MSG);
  }

  @Test
  public void echoRuntimeFail() throws TException {
    try {
      client.echoRuntimeFail(KITTY_MSG);
      Assert.fail("Thrift client did not throw an expected exception");
    } catch (Exception e) {
      Assert.assertEquals(TApplicationException.class.getName(), e.getClass().getName());
    }
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void echoPassHandler() {
    Assert.assertEquals(KITTY_MSG, handler.echoPass(KITTY_MSG));
  }

  @Test
  public void echoPass() throws TException {
    Assert.assertEquals(KITTY_MSG, client.echoPass(KITTY_MSG));
  }

  @Test
  public void onewayFailHandler() throws TException {
    exception.expect(TException.class);
    exception.expectCause(IsInstanceOf.instanceOf(UnsupportedOperationException.class));
    handler.onewayFail(KITTY_MSG);
  }

  @Test
  public void onewayFail() throws TException {
    client.onewayFail(KITTY_MSG);
    // verify normal two-way method still passes using same client
    echoPass();
  }

  @Test
  public void onewayRuntimeFailHandler() throws TException {
    exception.expect(UnsupportedOperationException.class);
    handler.onewayRuntimeFail(KITTY_MSG);
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
