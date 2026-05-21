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
package org.apache.accumulo.test.tracing;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Hex;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;

/**
 * Open telemetry tracing data sink for testing. Processes can send http/protobuf trace data to this
 * sink over http, and it will add them to an in memory queue that tests can read from.
 */
public class TraceCollector {
  private final Server server;

  private final LinkedBlockingQueue<SpanData> spanQueue = new LinkedBlockingQueue<>();

  private class TraceHandler extends Handler.Abstract {

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
      if (!request.getHttpURI().getPath().equals("/v1/traces")) {
        System.err.println("unexpected target : " + request.getHttpURI().getPath());
        response.setStatus(404);
        callback.succeeded();
        return true;
      }

      try (var in = Content.Source.asInputStream(request)) {
        var body = in.readAllBytes();
        var etsr =
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest.parseFrom(body);
        var spans =
            etsr.getResourceSpansList().stream().flatMap(r -> r.getScopeSpansList().stream())
                .flatMap(r -> r.getSpansList().stream()).toList();

        spans.forEach(s -> {
          var traceId = Hex.encodeHexString(s.getTraceId().toByteArray(), true);

          Map<String,String> stringAttrs = new HashMap<>();
          Map<String,Long> intAttrs = new HashMap<>();

          s.getAttributesList().forEach(kv -> {
            if (kv.getValue().hasIntValue()) {
              intAttrs.put(kv.getKey(), kv.getValue().getIntValue());
            } else if (kv.getValue().hasStringValue()) {
              stringAttrs.put(kv.getKey(), kv.getValue().getStringValue());
            }
          });

          spanQueue.add(
              new SpanData(traceId, s.getName(), Map.copyOf(stringAttrs), Map.copyOf(intAttrs)));
        });

      } catch (Throwable e) {
        e.printStackTrace();
        throw e;
      }

      response.setStatus(200);
      callback.succeeded();
      return true;
    }
  }

  TraceCollector(String host, int port) throws Exception {
    server = new Server(new InetSocketAddress(host, port));
    server.setHandler(new TraceHandler());
    server.start();
  }

  SpanData take() throws InterruptedException {
    return spanQueue.take();
  }

  void stop() throws Exception {
    server.stop();
  }
}
