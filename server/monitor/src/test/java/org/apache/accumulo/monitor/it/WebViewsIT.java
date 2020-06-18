/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.it;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyWriter;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.Monitor.MonitorFactory;
import org.apache.accumulo.monitor.view.WebViews;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.categories.MonitorTests;
import org.easymock.EasyMock;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Basic tests for parameter validation constraints
 */
@Category(MonitorTests.class)
@RunWith(PowerMockRunner.class)
@PrepareForTest({Monitor.class, Tables.class})
@PowerMockIgnore({"javax.xml.*"})
public class WebViewsIT extends JerseyTest {

  @Override
  public Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    ResourceConfig config = new ResourceConfig(WebViews.class);
    config.register(new MonitorFactory(monitor.get()));
    config.register(WebViewsIT.HashMapWriter.class);
    return config;
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(WebViewsIT.HashMapWriter.class);
  }

  private static AtomicReference<Monitor> monitor = new AtomicReference<>(null);

  @BeforeClass
  public static void createMocks() {
    ServerContext contextMock = EasyMock.createMock(ServerContext.class);
    expect(contextMock.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    expect(contextMock.getInstanceID()).andReturn("foo").atLeastOnce();
    expect(contextMock.getInstanceName()).andReturn("foo").anyTimes();

    Monitor monitorMock = EasyMock.createMock(Monitor.class);
    expect(monitorMock.getContext()).andReturn(contextMock).anyTimes();

    EasyMock.replay(contextMock, monitorMock);
    monitor.set(monitorMock);
  }

  @AfterClass
  public static void finishMocks() {
    Monitor m = monitor.get();
    verify(m.getContext(), m);
  }

  /**
   * Expect to fail the constraint validation on the REST endpoint. The constraint is the
   * pre-defined word character class Pattern so passing a table name with punctuation will cause a
   * 400 response code.
   */
  @Test
  public void testGetTablesConstraintViolations() {
    Response output = target("tables/f+o*o").request().get();
    assertEquals("should return status 400", 400, output.getStatus());
  }

  /**
   * Test path tables/{tableID} which passes constraints. On passing constraints underlying logic
   * will be executed so we need to mock a certain amount of it. Note: If you get test failures here
   * due to 500 server response, it's likely an underlying class or method call was added/modified
   * and needs mocking. Note: To get the proper response code back, you need to make sure jersey has
   * a registered MessageBodyWriter capable of serializing/writing the object returned from your
   * endpoint. We're using a simple stubbed out inner class HashMapWriter for this.
   *
   * @throws Exception
   *           not expected
   */
  @Test
  public void testGetTablesConstraintPassing() throws Exception {
    PowerMock.mockStatic(Tables.class);
    expect(Tables.getTableName(monitor.get().getContext(), TableId.of("foo"))).andReturn("bar");
    PowerMock.replayAll();

    // Using the mocks we can verify that the getModel method gets called via debugger
    // however it's difficult to continue to mock through the jersey MVC code for the properly built
    // response.
    // Our silly HashMapWriter registered in the configure method gets wired in and used here.
    Response output = target("tables/foo").request().get();
    assertEquals("should return status 200", 200, output.getStatus());
    String responseBody = output.readEntity(String.class);
    assertTrue(responseBody.contains("tableID=foo") && responseBody.contains("table=bar"));
  }

  /**
   * Test minutes parameter constraints. When outside range we should get a 400 response.
   */
  @Test
  public void testGetTracesSummaryValidationConstraint() {
    // Test upper bounds of constraint
    Response output = target("trace/summary").queryParam("minutes", 5000000).request().get();
    assertEquals("should return status 400", 400, output.getStatus());

    // Test lower bounds of constraint
    output = target("trace/summary").queryParam("minutes", -27).request().get();
    assertEquals("should return status 400", 400, output.getStatus());
  }

  /**
   * Silly stub to handle MessageBodyWriter for Hashmap. Registered in configure method and
   * auto-wired by Jersey.
   */
  public static class HashMapWriter implements MessageBodyWriter<HashMap<?,?>> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
        MediaType mediaType) {
      return true;
    }

    @Override
    public long getSize(HashMap<?,?> hashMap, Class<?> type, Type genericType,
        Annotation[] annotations, MediaType mediaType) {
      return 0;
    }

    @Override
    public void writeTo(HashMap<?,?> hashMap, Class<?> type, Type genericType,
        Annotation[] annotations, MediaType mediaType, MultivaluedMap<String,Object> httpHeaders,
        OutputStream entityStream) throws IOException, WebApplicationException {
      String s = hashMap.toString();
      entityStream.write(s.getBytes());
    }
  }
}
