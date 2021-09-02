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
package org.apache.accumulo.core.trace;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.ServiceLoader;

import org.apache.accumulo.core.trace.thrift.TInfo;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

public class TraceUtil {

  public static final String INSTRUMENTATION_NAME = "io.opentelemetry.contrib.accumulo";
  private static Tracer INSTANCE = null;

  public static synchronized Tracer getTracer() {
    if (INSTANCE == null) {
      ServiceLoader<TracerProvider> loader = ServiceLoader.load(TracerProvider.class);
      Optional<TracerProvider> first = loader.findFirst();
      if (first.isEmpty()) {
        // If no OpenTelemetry implementation on the ClassPath, then use the NOOP implementation
        INSTANCE = OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME);
      } else {
        INSTANCE = first.get().getTracer(INSTRUMENTATION_NAME);
      }
    }
    return INSTANCE;
  }

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current context. This is
   * used to send the current trace information to a remote process
   */
  public static TInfo traceInfo() {
    TInfo tinfo = new TInfo();
    GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), tinfo,
        (carrier, key, value) -> carrier.putToHeaders(key, value));
    return tinfo;
  }

  /**
   * Returns a newly created Context from the TInfo object sent by a remote process. The Context can
   * then be used in this process to continue the tracing. The Context is used like:
   *
   * <preformat> Context remoteCtx = TracerFactory.getContext(tinfo); Span span =
   * TracerFactory.getTracer().spanBuilder(name).setParent(remoteCtx).startSpan(); </preformat>
   *
   * @param tinfo
   *          tracing information
   * @return Context
   */
  public static Context getContext(TInfo tinfo) {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(),
        tinfo, new TextMapGetter<TInfo>() {
          @Override
          public Iterable<String> keys(TInfo carrier) {
            return carrier.getHeaders().keySet();
          }

          @Override
          public String get(TInfo carrier, String key) {
            return carrier.getHeaders().get(key);
          }
        });
  }

  /**
   * To move trace data from client to server, the RPC call must be annotated to take a TInfo object
   * as its first argument. The user can simply pass null, so long as they wrap their Client and
   * Service objects with these functions.
   *
   * <pre>
   * Trace.on(&quot;remoteMethod&quot;);
   * Iface c = new Client();
   * c = TracerFactory.wrapClient(c);
   * c.remoteMethod(null, arg2, arg3);
   * Trace.off();
   * </pre>
   *
   * The wrapper will see the annotated method and send or re-establish the trace information.
   *
   * Note that the result of these calls is a Proxy object that conforms to the basic interfaces,
   * but is not your concrete instance.
   */
  public static <T> T wrapClient(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      if (args == null || args.length < 1 || args[0] != null) {
        return method.invoke(instance, args);
      }
      if (TInfo.class.isAssignableFrom(method.getParameterTypes()[0])) {
        args[0] = traceInfo();
      }
      Span span = getTracer().spanBuilder("client:" + method.getName()).startSpan();
      try (Scope scope = span.makeCurrent()) {
        return method.invoke(instance, args);
      } catch (InvocationTargetException ex) {
        span.recordException(ex);
        span.setStatus(StatusCode.ERROR);
        throw ex.getCause();
      } finally {
        span.end();
      }
    };
    return wrapRpc(handler, instance);
  }

  public static <T> T wrapService(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      try {
        if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
          return method.invoke(instance, args);
        }
        TInfo tinfo = (TInfo) args[0];
        getContext(tinfo).makeCurrent();
        Span span = getTracer().spanBuilder(method.getName()).startSpan();
        try (Scope scope = span.makeCurrent()) {
          return method.invoke(instance, args);
        } finally {
          span.end();
        }
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    };
    return wrapRpc(handler, instance);
  }

  private static <T> T wrapRpc(final InvocationHandler handler, final T instance) {
    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

}
