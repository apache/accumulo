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
package org.apache.accumulo.core.trace;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ClassUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

public class TraceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(TraceUtil.class);

  private static final String SPAN_FORMAT = "%s::%s";

  private static volatile boolean enabled = true;

  public static void initializeTracer(AccumuloConfiguration conf) {
    enabled = conf.getBoolean(Property.GENERAL_OPENTELEMETRY_ENABLED);
    logTracingState();
  }

  private static void logTracingState() {
    var msg = "Trace enabled in Accumulo: {}, OpenTelemetry instance: {}, Tracer instance: {}";
    var enabledInAccumulo = enabled ? "yes" : "no";
    var openTelemetry = getOpenTelemetry();
    var tracer = getTracer(openTelemetry);
    LOG.info(msg, enabledInAccumulo, openTelemetry.getClass(), tracer.getClass());
  }

  private static OpenTelemetry getOpenTelemetry() {
    return enabled ? GlobalOpenTelemetry.get() : OpenTelemetry.noop();
  }

  private static Tracer getTracer(OpenTelemetry ot) {
    return ot.getTracer(Constants.APPNAME, Constants.VERSION);
  }

  public static Span startSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, null, null);
  }

  public static Span startSpan(Class<?> caller, String spanName, Map<String,String> attributes) {
    return startSpan(caller, spanName, null, attributes);
  }

  public static Span startClientRpcSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, SpanKind.CLIENT, null);
  }

  public static Span startFateSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, null, null);
  }

  public static Span startServerRpcSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, SpanKind.SERVER, null);
  }

  public static Span startServerRpcSpanFromContext(Class<?> caller, String spanName,
      Context parentContext) {
    if (!enabled && !Span.current().getSpanContext().isValid()) {
      return Span.getInvalid();
    }
    final String name = String.format(SPAN_FORMAT, caller.getSimpleName(), spanName);
    final SpanBuilder builder =
        getTracer(getOpenTelemetry()).spanBuilder(name).setSpanKind(SpanKind.SERVER);

    if (parentContext != null) {
      builder.setParent(parentContext);
    }

    return builder.startSpan();
  }

  private static Span startSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes) {
    if (!enabled && !Span.current().getSpanContext().isValid()) {
      return Span.getInvalid();
    }
    final String name = String.format(SPAN_FORMAT, caller.getSimpleName(), spanName);
    final SpanBuilder builder = getTracer(getOpenTelemetry()).spanBuilder(name);
    if (kind != null) {
      builder.setSpanKind(kind);
    }
    if (attributes != null) {
      attributes.forEach(builder::setAttribute);
    }
    return builder.startSpan();
  }

  /**
   * Extract the trace context from the given map.
   *
   * @param headers the map containing the trace context
   * @return the extracted trace context
   */
  public static Context extractTraceContext(Map<String,String> headers) {
    return W3CTraceContextPropagator.getInstance().extract(Context.current(), headers,
        new TextMapGetter<>() {
          @Override
          public Iterable<String> keys(@NonNull Map<String,String> carrier) {
            return carrier.keySet();
          }

          @Override
          public String get(Map<String,String> carrier, @NonNull String key) {
            return carrier.get(key);
          }
        });
  }

  /**
   * @return the serialized String version of the given Context in the format
   *         "count|key|value|key|value|..."
   */
  public static String serializeContext(Context context) {
    Map<String,String> traceHeaders = new HashMap<>();
    W3CTraceContextPropagator.getInstance().inject(context, traceHeaders, (headers, key, value) -> {
      if (headers != null) {
        headers.put(key, value);
      }
    });

    StringBuilder sb = new StringBuilder();
    sb.append(traceHeaders.size());

    for (var entry : traceHeaders.entrySet()) {
      sb.append('|').append(entry.getKey()).append('|').append(entry.getValue());
    }

    return sb.toString();
  }

  /**
   * @return the deserialized Context from the given String
   */
  public static Context deserializeContext(String serializedContext) {
    if (serializedContext == null || serializedContext.isEmpty()) {
      return Context.current();
    }

    String[] parts = serializedContext.split("\\|");
    if (parts.length == 0) {
      LOG.debug("Empty parts array in deserializeContext");
      return Context.current();
    }

    int count;
    try {
      count = Integer.parseInt(parts[0]);
    } catch (NumberFormatException e) {
      LOG.debug("Failed to parse trace header count in context: {}", serializedContext, e);
      return Context.current();
    }

    if (count == 0) {
      LOG.debug("Empty trace context, returning current context");
      return Context.current();
    }

    int expectedLength = 1 + (2 * count);
    if (parts.length < expectedLength) {
      LOG.debug("Incomplete serialized context. Expected {} parts but found {}", expectedLength,
          parts.length);
      return Context.current();
    }

    Map<String,String> headers = new HashMap<>(count);
    for (int i = 0; i < count; i++) {
      int keyIndex = 1 + (i * 2);
      int valueIndex = keyIndex + 1;
      headers.put(parts[keyIndex], parts[valueIndex]);
    }

    return extractTraceContext(headers);
  }

  /**
   * Record that an Exception occurred in the code covered by a Span
   *
   * @param span the span
   * @param e the exception
   * @param rethrown whether the exception is subsequently re-thrown
   */
  public static void setException(Span span, Throwable e, boolean rethrown) {
    if (enabled) {
      span.setStatus(StatusCode.ERROR);
      span.recordException(e,
          Attributes.builder().put(AttributeKey.stringKey("exception.type"), e.getClass().getName())
              .put(AttributeKey.stringKey("exception.message"), e.getMessage())
              .put(AttributeKey.booleanKey("exception.escaped"), rethrown).build());
    }
  }

  public static Runnable wrap(Runnable r) {
    return r instanceof TraceWrappedRunnable ? r : new TraceWrappedRunnable(r);
  }

  public static Runnable unwrap(Runnable r) {
    return TraceWrappedRunnable.unwrapFully(r);
  }

  public static <T> Callable<T> wrap(Callable<T> c) {
    return c instanceof TraceWrappedCallable ? c : new TraceWrappedCallable<>(c);
  }

  public static <T> Callable<T> unwrap(Callable<T> c) {
    return TraceWrappedCallable.unwrapFully(c);
  }

  public static <T> T wrapService(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      Span span = Span.current(); // should be set by protocol
      try {
        return method.invoke(instance, args);
      } catch (Exception e) {
        Throwable t = e instanceof InvocationTargetException ? e.getCause() : e;
        setException(span, t, true);
        throw t;
      }
    };
    return wrapRpc(handler, instance);
  }

  private static <T> T wrapRpc(final InvocationHandler handler, final T instance) {
    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        ClassUtil.getInterfaces(instance.getClass()).toArray(new Class<?>[0]), handler);
    return proxiedInstance;
  }

}
