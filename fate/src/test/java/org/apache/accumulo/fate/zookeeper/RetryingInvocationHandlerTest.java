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
package org.apache.accumulo.fate.zookeeper;

import java.lang.reflect.Method;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class RetryingInvocationHandlerTest {
  private static final String PATH = "/path/to/somewhere";
  private static final byte[] DATA = {(byte) 1, (byte) 2};
  private static final Object[] ARGS = {PATH, DATA};
  private static final String RV = "OK";

  private static Method putMethod;

  @BeforeClass
  public static void setUpClass() throws Exception {
    putMethod = IZooReaderWriter.class.getMethod("putEphemeralData", String.class, byte[].class);
  }

  private IZooReaderWriter zrw;
  private RetryingInvocationHandler ih;

  @Before
  public void setUp() throws Exception {
    zrw = createMock(IZooReaderWriter.class);
    ih = new RetryingInvocationHandler(zrw);
  }

  @Test
  public void testInvokeSuccessful() throws Throwable {
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andReturn(RV);
    replay(zrw);
    Object rv = ih.invoke(null, putMethod, ARGS);
    verify(zrw);
    assertEquals(RV, rv);
  }

  @Test
  public void testInvokeRetrySuccessful() throws Throwable {
    ConnectionLossException e = createMock(ConnectionLossException.class);
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andThrow(e);
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andThrow(e);
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andReturn(RV);
    replay(zrw);
    Object rv = ih.invoke(null, putMethod, ARGS);
    verify(zrw);
    assertEquals(RV, rv);
  }

  @Test(expected = InterruptedException.class)
  public void testInvokeRetryFailure() throws Throwable {
    ConnectionLossException e = createMock(ConnectionLossException.class);
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andThrow(e);
    expect(zrw.putEphemeralData(eq(PATH), aryEq(DATA))).andThrow(new InterruptedException());
    replay(zrw);
    try {
      ih.invoke(null, putMethod, ARGS);
    } finally {
      verify(zrw);
    }
  }
}
