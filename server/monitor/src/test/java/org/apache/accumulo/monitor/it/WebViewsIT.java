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
package org.apache.accumulo.monitor.it;

import static org.apache.accumulo.monitor.it.TagNameConstants.MONITOR;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.MessageBodyWriter;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.Monitor.MonitorFactory;
import org.apache.accumulo.monitor.view.WebViews;
import org.apache.accumulo.server.ServerContext;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Basic tests for parameter validation constraints
 */
@Tag(MONITOR)
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

  @BeforeAll
  public static void createMocks() throws TableNotFoundException {
    ServerContext contextMock = createMock(ServerContext.class);
    expect(contextMock.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    expect(contextMock.getInstanceID()).andReturn(InstanceId.of("foo")).atLeastOnce();
    expect(contextMock.getInstanceName()).andReturn("foo").anyTimes();
    expect(contextMock.getZooKeepers()).andReturn("foo:2181").anyTimes();
    expect(contextMock.getTableName(TableId.of("foo"))).andReturn("bar").anyTimes();

    Monitor monitorMock = createMock(Monitor.class);
    expect(monitorMock.getContext()).andReturn(contextMock).anyTimes();

    replay(contextMock, monitorMock);
    monitor.set(monitorMock);
  }

  @AfterAll
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
    assertEquals(400, output.getStatus(), "should return status 400");
  }

  /**
   * Test path tables/{tableID} which passes constraints. On passing constraints underlying logic
   * will be executed so we need to mock a certain amount of it. Note: If you get test failures here
   * due to 500 server response, it's likely an underlying class or method call was added/modified
   * and needs mocking. Note: To get the proper response code back, you need to make sure jersey has
   * a registered MessageBodyWriter capable of serializing/writing the object returned from your
   * endpoint. We're using a simple stubbed out inner class HashMapWriter for this.
   *
   * @throws Exception not expected
   */
  @Test
  public void testGetTablesConstraintPassing() throws Exception {
    // Using the mocks we can verify that the getModel method gets called via debugger
    // however it's difficult to continue to mock through the jersey MVC code for the properly built
    // response.
    // Our silly HashMapWriter registered in the configure method gets wired in and used here.
    Response output = target("tables/foo").request().get();
    assertEquals(200, output.getStatus(), "should return status 200");
    String responseBody = output.readEntity(String.class);
    assertTrue(responseBody.contains("tableID=foo") && responseBody.contains("table=bar"));
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
