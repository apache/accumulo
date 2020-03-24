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
package org.apache.accumulo.tracer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.tracer.thrift.TestService;
import org.apache.accumulo.tracer.thrift.TestService.Iface;
import org.apache.accumulo.tracer.thrift.TestService.Processor;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.Tracer;
import org.apache.htrace.wrappers.TraceProxy;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TracerTest {
  static class SpanStruct {
    public SpanStruct(long traceId, long spanId, List<Long> parentIds, long start, long stop,
        String description, Map<String,String> data) {
      super();
      this.traceId = traceId;
      this.spanId = spanId;
      this.parentIds = parentIds;
      this.start = start;
      this.stop = stop;
      this.description = description;
      this.data = data;
    }

    public long traceId;
    public long spanId;
    public List<Long> parentIds;
    public long start;
    public long stop;
    public String description;
    public Map<String,String> data;

    public long millis() {
      return stop - start;
    }
  }

  static class TestReceiver implements SpanReceiver {
    public Map<Long,List<SpanStruct>> traces = new HashMap<>();

    public TestReceiver() {}

    @Override
    public void receiveSpan(Span s) {
      long traceId = s.getTraceId();
      SpanStruct span = new SpanStruct(traceId, s.getSpanId(), Longs.asList(s.getParents()),
          s.getStartTimeMillis(), s.getStopTimeMillis(), s.getDescription(), s.getKVAnnotations());
      if (!traces.containsKey(traceId))
        traces.put(traceId, new ArrayList<>());
      traces.get(traceId).add(span);
    }

    @Override
    public void close() {}
  }

  @Test
  public void testTrace() throws Exception {
    TestReceiver tracer = new TestReceiver();
    Trace.addReceiver(tracer);

    assertFalse(Trace.isTracing());
    try (TraceScope span = Trace.startSpan("nop")) {
      // do nothing
    }
    assertEquals(0, tracer.traces.size());
    assertFalse(Trace.isTracing());

    try (TraceScope span = Trace.startSpan("nop", Sampler.ALWAYS)) {
      // do nothing
    }
    assertEquals(1, tracer.traces.size());
    assertFalse(Trace.isTracing());

    Trace.startSpan("testing", Sampler.ALWAYS);
    assertTrue(Trace.isTracing());

    try (TraceScope span = Trace.startSpan("shortest trace ever")) {
      // do nothing
    }
    long traceId = Trace.currentSpan().getTraceId();
    assertNotNull(tracer.traces.get(traceId));
    assertEquals(1, tracer.traces.get(traceId).size());
    assertEquals("shortest trace ever", tracer.traces.get(traceId).get(0).description);

    try (TraceScope pause = Trace.startSpan("pause")) {
      Thread.sleep(100);
    }
    assertEquals(2, tracer.traces.get(traceId).size());
    assertTrue(tracer.traces.get(traceId).get(1).millis() >= 100);

    Thread t = new Thread(Trace.wrap(() -> assertTrue(Trace.isTracing())), "My Task");
    t.start();
    t.join();

    assertEquals(3, tracer.traces.get(traceId).size());
    assertEquals("My Task", tracer.traces.get(traceId).get(2).description);
    Trace.currentSpan().stop();
    Tracer.getInstance().continueSpan(null);
    assertFalse(Trace.isTracing());
  }

  static class Service implements TestService.Iface {
    @Override
    public boolean checkTrace(TInfo t, String message) {
      try (TraceScope trace = Trace.startSpan(message)) {
        return Trace.isTracing();
      }
    }
  }

  @SuppressFBWarnings(value = {"UNENCRYPTED_SOCKET", "UNENCRYPTED_SERVER_SOCKET"},
      justification = "insecure, known risk, test socket")
  @Test
  public void testThrift() throws Exception {
    TestReceiver tracer = new TestReceiver();
    Trace.addReceiver(tracer);

    ServerSocket socket = new ServerSocket(0);
    TServerSocket transport = new TServerSocket(socket);
    transport.listen();
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
    args.processor(new Processor<Iface>(TraceUtil.wrapService(new Service())));
    final TServer tserver = new TThreadPoolServer(args);
    Thread t = new Thread(tserver::serve);
    t.start();
    TTransport clientTransport = new TSocket(new Socket("localhost", socket.getLocalPort()));
    TestService.Iface client = new TestService.Client(new TBinaryProtocol(clientTransport),
        new TBinaryProtocol(clientTransport));
    client = TraceUtil.wrapClient(client);
    assertFalse(client.checkTrace(null, "test"));

    long startTraceId;
    try (TraceScope start = Trace.startSpan("start", Sampler.ALWAYS)) {
      assertTrue(client.checkTrace(null, "my test"));
      startTraceId = start.getSpan().getTraceId();
    }

    assertNotNull(tracer.traces.get(startTraceId));
    String[] traces = {"my test", "checkTrace", "client:checkTrace", "start"};
    assertEquals(tracer.traces.get(startTraceId).size(), traces.length);
    for (int i = 0; i < traces.length; i++)
      assertEquals(traces[i], tracer.traces.get(startTraceId).get(i).description);

    tserver.stop();
    t.join(100);
  }

  Callable<Object> callable;

  @Before
  public void setup() {
    callable = () -> {
      throw new IOException();
    };
  }

  /**
   * Verify that exceptions propagate up through the trace wrapping with sampling enabled, as the
   * cause of the reflexive exceptions.
   */
  @Test(expected = IOException.class)
  public void testTracedException() throws Throwable {
    try {
      TraceProxy.trace(callable).call();
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * Verify that exceptions propagate up through the trace wrapping with sampling disabled, as the
   * cause of the reflexive exceptions.
   */
  @Test(expected = IOException.class)
  public void testUntracedException() throws Throwable {
    try {
      TraceProxy.trace(callable, Sampler.NEVER).call();
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
