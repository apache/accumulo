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
package org.apache.accumulo.start.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncSocketAppenderTest {
  private SocketAppender sa;
  private AsyncSocketAppender asa;

  @Before
  public void setUp() throws Exception {
    sa = EasyMock.createMock(SocketAppender.class);
  }

  @Test
  public void testDelegates() {
    asa = new AsyncSocketAppender();
    asa.setApplication("myapp");
    asa.setLocationInfo(true);
    asa.setPort(1234);
    asa.setReconnectionDelay(56);
    asa.setRemoteHost("remotehost");
    Assert.assertEquals("myapp", asa.getApplication());
    Assert.assertEquals(true, asa.getLocationInfo()); // not really delegating
    Assert.assertEquals(1234, asa.getPort());
    Assert.assertEquals(56, asa.getReconnectionDelay());
    Assert.assertEquals("remotehost", asa.getRemoteHost());
  }

  @Test
  public void testSetLocationInfo() {
    sa.setLocationInfo(true);
    EasyMock.replay(sa);
    asa = new AsyncSocketAppender(sa);
    asa.setLocationInfo(true);
    EasyMock.verify(sa);
  }

  @Test
  public void testAppend() {
    asa = new AsyncSocketAppender(sa);
    Assert.assertFalse(asa.isAttached(sa));
    LoggingEvent event1 = new LoggingEvent("java.lang.String", Logger.getRootLogger(), Level.INFO, "event1", null);
    LoggingEvent event2 = new LoggingEvent("java.lang.Integer", Logger.getRootLogger(), Level.WARN, "event2", null);
    sa.activateOptions();
    sa.doAppend(event1);
    sa.doAppend(event2);
    sa.close();
    EasyMock.replay(sa);
    asa.doAppend(event1);
    asa.doAppend(event2);
    asa.close(); // forces events to be appended to socket appender
    Assert.assertTrue(asa.isAttached(sa));
    EasyMock.verify(sa);
  }
}
