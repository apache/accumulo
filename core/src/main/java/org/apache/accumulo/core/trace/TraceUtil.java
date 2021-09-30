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
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.trace.OpenTelemetryFactory;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

public class TraceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(TraceUtil.class);

  private static final String SPAN_FORMAT = "%s::%s";

  private static Tracer instance = null;
  private static String name = null;
  private static boolean tracing = false;

  private static void initializeInternals(OpenTelemetry ot, String instrumentationName) {
    // TODO: Is there a way to get our version to pass to getTracer() ?
    name = instrumentationName;
    instance = ot.getTracer(name);
    tracing = (!ot.equals(OpenTelemetry.noop()));
    LOG.info("Trace enabled: {}, OpenTelemetry instance: {}, Tracer instance: {}", tracing,
        ot.getClass(), instance.getClass());
  }

  /**
   * Initialize TracerUtil using the OpenTelemetry parameter and instrumentationName
   *
   * @param ot
   *          OpenTelemetry instance
   * @param instrumentationName
   *          OpenTelemetry instrumentation library name
   */
  public static void initializeTracer(OpenTelemetry ot, String instrumentationName) {
    if (instance != null) {
      initializeInternals(ot, instrumentationName);
    } else {
      LOG.warn("Tracer already initialized.");
    }
  }

  /**
   * Use the property values in the AccumuloConfiguration to call
   * {@link #initializeTracer(boolean, String, String)}
   *
   * @param conf
   *          AccumuloConfiguration
   * @throws Exception
   *           unable to find or load class
   */
  public static void initializeTracer(AccumuloConfiguration conf) throws Exception {
    initializeTracer(conf.getBoolean(Property.GENERAL_OPENTELEMETRY_ENABLED),
        conf.get(Property.GENERAL_OPENTELEMETRY_FACTORY),
        conf.get(Property.GENERAL_OPENTELEMETRY_NAME));
  }

  /**
   * If not enabled, the OpenTelemetry implementation will be set to the NoOp implementation. If
   * enabled and a factoryClass is supplied, then we will get the OpenTelemetry instance from the
   * factory class.
   *
   * @param enabled
   *          whether or not tracing is enabled
   * @param factoryClass
   *          name of class to load
   * @param instrumentationName
   *          OpenTelemetry instrumentation library name
   * @throws Exception
   *           unable to find or load class
   */
  public static void initializeTracer(boolean enabled, String factoryClass,
      String instrumentationName) throws Exception {
    if (instance == null) {
      OpenTelemetry ot = null;
      if (!enabled) {
        ot = OpenTelemetry.noop();
      } else if (factoryClass != null && !factoryClass.isEmpty()) {
        Class<? extends OpenTelemetryFactory> clazz =
            (Class<? extends OpenTelemetryFactory>) ClassLoaderUtil.loadClass(factoryClass,
                OpenTelemetryFactory.class);
        OpenTelemetryFactory factory = clazz.getDeclaredConstructor().newInstance();
        ot = factory.getOpenTelemetry();
        LOG.info("OpenTelemetry configured and set from {}", clazz);
      } else {
        ot = GlobalOpenTelemetry.get();
      }
      initializeInternals(ot, instrumentationName);
    } else {
      LOG.warn("Tracer already initialized.");
    }
  }

  /**
   * @return the Tracer set on the GlobalOpenTelemetry object
   */
  private static Tracer getTracer() {
    if (Objects.isNull(instance)) {
      LOG.warn("initializeTracer not called, using GlobalOpenTelemetry.getTracer()");
      instance = GlobalOpenTelemetry.getTracer(name);
      tracing = (!instance.equals(OpenTelemetry.noop().getTracer(name)));
      LOG.info("Trace enabled: {}, Tracer is: {}", tracing, instance.getClass());
    }
    return instance;
  }

  /**
   * @return true if an OpenTelemetry Tracer implementation has been set, false if the NoOp Tracer
   *         is being used.
   */
  public static boolean isTracing() {
    return tracing;
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind) {
    return createSpan(caller, spanName, kind, null, false, null);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind, Context parent) {
    return createSpan(caller, spanName, kind, null, false, parent);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes) {
    return createSpan(caller, spanName, kind, attributes, false, null);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes, boolean setNoParent, Context parent) {
    final String name = String.format(SPAN_FORMAT, caller.getSimpleName(), spanName);
    final SpanBuilder builder = getTracer().spanBuilder(name);
    builder.setSpanKind(kind);
    if (attributes != null) {
      attributes.forEach((k, v) -> builder.setAttribute(k, v));
    }
    if (setNoParent) {
      builder.setNoParent();
    } else if (parent != null) {
      builder.setParent(parent);
    }
    return builder.startSpan();
  }

  /**
   * Record that an Exception occurred in the code covered by a Span
   *
   * @param span
   *          the span
   * @param e
   *          the exception
   * @param rethrown
   *          whether the exception is subsequently re-thrown
   */
  public static void setException(Span span, Throwable e, boolean rethrown) {
    if (tracing) {
      span.setStatus(StatusCode.ERROR);
      span.recordException(e,
          Attributes.builder().put(SemanticAttributes.EXCEPTION_TYPE, e.getClass().getName())
              .put(SemanticAttributes.EXCEPTION_MESSAGE, e.getMessage())
              .put(SemanticAttributes.EXCEPTION_ESCAPED, rethrown).build());
    }
  }

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current context. This is
   * used to send the current trace information to a remote process
   */
  public static TInfo traceInfo() {
    TInfo tinfo = new TInfo();
    W3CTraceContextPropagator.getInstance().inject(Context.current(), tinfo,
        (carrier, key, value) -> carrier.putToHeaders(key, value));
    return tinfo;
  }

  /**
   * Returns a newly created Context from the TInfo object sent by a remote process. The Context can
   * then be used in this process to continue the tracing. The Context is used like:
   *
   * <pre>
   * Context remoteCtx = TracerFactory.getContext(tinfo);
   * Span span = TracerFactory.getTracer().spanBuilder(name).setParent(remoteCtx).startSpan()
   * </pre>
   *
   * @param tinfo
   *          tracing information
   * @return Context
   */
  public static Context getContext(TInfo tinfo) {
    return W3CTraceContextPropagator.getInstance().extract(Context.current(), tinfo,
        new TextMapGetter<TInfo>() {
          @Override
          public Iterable<String> keys(TInfo carrier) {
            return carrier.getHeaders().keySet();
          }

          @Override
          public String get(TInfo carrier, String key) {
            if (carrier.getHeaders() == null) {
              return null;
            }
            return carrier.getHeaders().get(key);
          }
        });
  }

  public static <T> T wrapService(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      try {
        if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
          return method.invoke(instance, args);
        }
        TInfo tinfo = (TInfo) args[0];
        getContext(tinfo).makeCurrent();
        Span span = createSpan(instance.getClass(), method.getName(), SpanKind.SERVER);
        try (Scope scope = span.makeCurrent()) {
          return method.invoke(instance, args);
        } catch (Exception e) {
          setException(span, e, true);
          throw e;
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
