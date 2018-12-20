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

import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.trace.wrappers.TraceWrap;
import org.apache.accumulo.tracer.thrift.TestService;
import org.apache.accumulo.tracer.thrift.TestService.Iface;
import org.apache.accumulo.tracer.thrift.TestService.Processor;
import org.apache.htrace.Sampler;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.wrappers.TraceProxy;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TracerTest {
  static class SpanStruct {
    public SpanStruct(long traceId, long spanId, long parentId, long start, long stop,
        String description, Map<byte[],byte[]> data) {
      super();
      this.traceId = traceId;
      this.spanId = spanId;
      this.parentId = parentId;
      this.start = start;
      this.stop = stop;
      this.description = description;
      this.data = data;
    }

    public long traceId;
    public long spanId;
    public long parentId;
    public long start;
    public long stop;
    public String description;
    public Map<byte[],byte[]> data;

    public long millis() {
      return stop - start;
    }
  }

  static class TestReceiver implements SpanReceiver {
    public Map<Long,List<SpanStruct>> traces = new HashMap<>();

    public TestReceiver() {}

    @Override
    public void receiveSpan(org.apache.htrace.Span s) {
      long traceId = s.getTraceId();
      SpanStruct span = new SpanStruct(traceId, s.getSpanId(), s.getParentId(),
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
    org.apache.htrace.Trace.addReceiver(tracer);

    assertFalse(Trace.isTracing());
    Trace.start("nop").stop();
    assertEquals(0, tracer.traces.size());
    assertFalse(Trace.isTracing());

    Trace.on("nop").stop();
    assertEquals(1, tracer.traces.size());
    assertFalse(Trace.isTracing());

    Trace.on("testing");
    assertTrue(Trace.isTracing());

    Span span = Trace.start("shortest trace ever");
    span.stop();
    long traceId = Trace.currentTraceId();
    assertNotNull(tracer.traces.get(traceId));
    assertEquals(1, tracer.traces.get(traceId).size());
    assertEquals("shortest trace ever", tracer.traces.get(traceId).get(0).description);

    Span pause = Trace.start("pause");
    Thread.sleep(100);
    pause.stop();
    assertEquals(2, tracer.traces.get(traceId).size());
    assertTrue(tracer.traces.get(traceId).get(1).millis() >= 100);

    Thread t = new Thread(Trace.wrap(new Runnable() {
      @Override
      public void run() {
        assertTrue(Trace.isTracing());
      }
    }), "My Task");
    t.start();
    t.join();

    assertEquals(3, tracer.traces.get(traceId).size());
    assertEquals("My Task", tracer.traces.get(traceId).get(2).description);
    Trace.off();
    assertFalse(Trace.isTracing());
  }

  static class Service implements TestService.Iface {
    @Override
    public boolean checkTrace(TInfo t, String message) {
      Span trace = Trace.start(message);
      try {
        return Trace.isTracing();
      } finally {
        trace.stop();
      }
    }
  }

  @SuppressFBWarnings(value = {"UNENCRYPTED_SOCKET", "UNENCRYPTED_SERVER_SOCKET"},
      justification = "insecure, known risk, test socket")
  @Test
  public void testThrift() throws Exception {
    TestReceiver tracer = new TestReceiver();
    org.apache.htrace.Trace.addReceiver(tracer);

    ServerSocket socket = new ServerSocket(0);
    TServerSocket transport = new TServerSocket(socket);
    transport.listen();
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
    args.processor(new Processor<Iface>(TraceWrap.service(new Service())));
    final TServer tserver = new TThreadPoolServer(args);
    Thread t = new Thread() {
      @Override
      public void run() {
        tserver.serve();
      }
    };
    t.start();
    TTransport clientTransport = new TSocket(new Socket("localhost", socket.getLocalPort()));
    TestService.Iface client = new TestService.Client(new TBinaryProtocol(clientTransport),
        new TBinaryProtocol(clientTransport));
    client = TraceWrap.client(client);
    assertFalse(client.checkTrace(null, "test"));

    Span start = Trace.on("start");
    assertTrue(client.checkTrace(null, "my test"));
    start.stop();

    assertNotNull(tracer.traces.get(start.traceId()));
    String traces[] = {"my test", "checkTrace", "client:checkTrace", "start"};
    assertEquals(tracer.traces.get(start.traceId()).size(), traces.length);
    for (int i = 0; i < traces.length; i++)
      assertEquals(traces[i], tracer.traces.get(start.traceId()).get(i).description);

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
