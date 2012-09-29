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
package org.apache.accumulo.cloudtrace.instrument.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.cloudtrace.thrift.TInfo;


/**
 * To move trace data from client to server, the RPC call must be annotated to take a TInfo object as its first argument. The user can simply pass null, so long
 * as they wrap their Client and Service objects with these functions.
 * 
 * <pre>
 * Trace.on(&quot;remoteMethod&quot;);
 * Iface c = new Client();
 * c = TraceWrap.client(c);
 * c.remoteMethod(null, arg2, arg3);
 * Trace.off();
 * </pre>
 * 
 * The wrapper will see the annotated method and send or re-establish the trace information.
 * 
 * Note that the result of these calls is a Proxy object that conforms to the basic interfaces, but is not your concrete instance.
 * 
 */
public class TraceWrap {
  
  @SuppressWarnings("unchecked")
  public static <T> T service(final T instance) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
          return method.invoke(instance, args);
        }
        Span span = Trace.trace((TInfo) args[0], method.getName());
        try {
          return method.invoke(instance, args);
        } catch (InvocationTargetException ex) {
          throw ex.getCause();
        } finally {
          span.stop();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
  }
  
  @SuppressWarnings("unchecked")
  public static <T> T client(final T instance) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        if (args == null || args.length < 1 || args[0] != null) {
          return method.invoke(instance, args);
        }
        Class<?> klass = method.getParameterTypes()[0];
        if (TInfo.class.isAssignableFrom(klass)) {
          args[0] = Tracer.traceInfo();
        }
        Span span = Trace.start("client:" + method.getName());
        try {
          return method.invoke(instance, args);
        } catch (InvocationTargetException ex) {
          throw ex.getCause();
        } finally {
          span.stop();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
  }
  
}
