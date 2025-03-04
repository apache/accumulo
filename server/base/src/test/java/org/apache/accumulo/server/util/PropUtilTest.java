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
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PropUtilTest {

  private static final String TEST_CONTEXT_NAME = "TestContext";
  private static final String INVALID_TEST_CONTEXT_NAME = "InvalidTestContext";

  public static class TestContextClassLoaderFactory implements ContextClassLoaderFactory {

    @Override
    public ClassLoader getClassLoader(String contextName) {
      if (contextName.equals(TEST_CONTEXT_NAME)) {
        return this.getClass().getClassLoader();
      }
      return null;
    }

  }

  @BeforeEach
  public void before() {
    ClassLoaderUtil.resetContextFactoryForTests();
  }

  @Test
  public void testSetClasspathContextFails() {
    ServerContext ctx = createMock(ServerContext.class);
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    InstanceId iid = createMock(InstanceId.class);
    TableId tid = createMock(TableId.class);
    TablePropKey tkey = TablePropKey.of(tid);

    expect(ctx.getConfiguration()).andReturn(conf).once();
    expect(conf.get(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY))
        .andReturn(TestContextClassLoaderFactory.class.getName());

    replay(ctx, conf, iid, tid);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> PropUtil.validateProperties(ctx, tkey,
            Map.of(Property.TABLE_CLASSLOADER_CONTEXT.getKey(), INVALID_TEST_CONTEXT_NAME)));
    assertEquals("Unable to resolve classloader for context: " + INVALID_TEST_CONTEXT_NAME,
        e.getMessage());
    verify(ctx, conf, iid, tid);
  }

  @Test
  public void testSetClasspathContext() {
    ServerContext ctx = createMock(ServerContext.class);
    AccumuloConfiguration conf = createMock(AccumuloConfiguration.class);
    InstanceId iid = createMock(InstanceId.class);
    TableId tid = createMock(TableId.class);
    TablePropKey tkey = TablePropKey.of(tid);

    expect(ctx.getConfiguration()).andReturn(conf).once();
    expect(conf.get(Property.GENERAL_CONTEXT_CLASSLOADER_FACTORY))
        .andReturn(TestContextClassLoaderFactory.class.getName());

    replay(ctx, conf, iid, tid);
    PropUtil.validateProperties(ctx, tkey,
        Map.of(Property.TABLE_CLASSLOADER_CONTEXT.getKey(), TEST_CONTEXT_NAME));
    verify(ctx, conf, iid, tid);
  }

}
